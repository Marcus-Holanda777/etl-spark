# Databricks notebook source
from utils_superdesconto import view_autorizador, view_cupom, view_produto
from datetime import datetime
from dateutil.relativedelta import relativedelta
from athena_mvsh import CursorParquetDuckdb, Athena
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import col
from utils_ressarcimento import main_view_ressarcimento
from dotenv import dotenv_values
from pyspark.sql import DataFrame


def cast_decimal_double(df: DataFrame, *timestamp) -> DataFrame:
    columns = {
        c.name: col(c.name).cast(T.DoubleType())
        for c in df.schema
        if isinstance(c.dataType, T.DecimalType)
    }

    for c in timestamp:
        columns |= {c: col(c).cast(T.TimestampNTZType())}

    return df.withColumns(columns)


def main_executor(
    spark: SparkSession, start: datetime, end: datetime, config: dict[str, str]
) -> None:
    autorizador = view_autorizador(spark, config)
    produto = view_produto(spark, config)
    cupom = view_cupom(spark, start, end, config)

    view_create = (
        F.broadcast(cupom)
        .join(autorizador, cupom.etiqueta == autorizador.ulch_cd_barras)
        .join(produto, autorizador.ulch_sq_produto == produto.ulch_sq_produto)
        .select(
            autorizador.ulch_sq_autorizacao,
            produto.ulch_sq_produto,
            produto.xxxx_dh_cad,
            cupom.periodo.alias("dt_venda"),
            cupom.filial,
            cupom.cod_prod,
            produto.ulch_lote,
            produto.ulch_dt_vencimento,
            cupom.etiqueta,
            cupom.perc_dsc_cupom,
            cupom.venda,
            cupom.venda_desconto,
            autorizador.ulch_preco_venda.alias("ulch_preco_venda"),
            autorizador.ulch_percentual_desconto,
            autorizador.ulch_fl_tipo_produto,
        )
    )

    # ETL -- athena
    cursor = CursorParquetDuckdb(
        s3_staging_dir=config.get("location"),
        result_reuse_enable=True,
        aws_access_key_id=config.get("username"),
        aws_secret_access_key=config.get("password"),
        region_name=config.get("region"),
    )

    print(f"periodo: {start:%Y-%m-%d} - {end:%Y-%m-%d}")
    df = cast_decimal_double(view_create, "ulch_dt_vencimento").toPandas()

    print(f"Rows df: {df.shape}")

    # --  bases do athena
    location_tables = config.get("location_tables")
    schema_athena = config.get("schema_athena")
    table_athena = config.get("table_athena")
    table_athena_ressarcimento = config.get("table_athena_ressarcimento")

    with Athena(cursor=cursor) as cliente:
        cliente.merge_table_iceberg(
            table_athena,
            df,
            schema=schema_athena,
            predicate="t.etiqueta = s.etiqueta",
            location=f"{location_tables}tables/{table_athena}/",
        )

        # -- RESSARCIMENTO ENTRE 1 E 5
        # atualizar tabelas entre os dias 1 e 5 de cada mes
        day_ressarc = end.day

        if 1 <= day_ressarc <= 5:
            print("Base ressarcimento !")
            view_ressarc = main_view_ressarcimento(spark, end.year, config)

            # adiciona mais uma etapa ao pyspark
            df_ressarc = cast_decimal_double(view_ressarc).toPandas()

            if not df_ressarc.empty():
                print(f"Rows df: {df_ressarc.shape}")
                cliente.write_dataframe(
                    df_ressarc,
                    table_name=table_athena_ressarcimento,
                    location=f"{location_tables}tables/{table_athena_ressarcimento}/",
                    schema=schema_athena,
                )
            else:
                print("DF, vazio !")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Jobs_pmenos").getOrCreate()

    # definir periodo
    end = datetime.now()
    start = end.replace(day=1)

    if end.day <= 5:
        start = start - relativedelta(months=1)

    # definir acesso externo
    config = {**dotenv_values()}

    main_executor(spark, start, end, config)
