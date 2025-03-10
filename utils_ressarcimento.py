from pyspark.sql import DataFrame, Column, SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import col


def strip_normalize(colname: str | Column) -> Column:
    """normalizar texto"""
    target = "ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ"
    source = "aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ"
    return F.upper(
        F.regexp_replace(F.translate(F.trim(colname), target, source), r" +", " ")
    )


def cnpj_normalize(colname: str | Column) -> Column:
    """normalizar CNPJ dos fornecedores"""
    return F.lpad(F.regexp_replace(F.trim(colname), r"[^0-9a-zA-Z]", ""), 15, "0")


def view_fornecedores(spark: SparkSession, config: dict[str, str]) -> DataFrame:
    """
    :Remover a duplicidade do codigo filho
    :considerando o ultimo cadastro pai feito pro fornecedor
    :fornecedor comercial base do SAPHANA
    """

    forn = spark.read.parquet(config.get("fornecedor"))
    forn_pai = spark.read.parquet(config.get("aporte_cab"))
    forn_filho = spark.read.parquet(config.get("aporte_det"))

    forn_comercial = (
        spark.read.schema(
            T.StructType(
                [
                    T.StructField("codigo_fornecedor_principal_deposito", T.LongType()),
                    T.StructField("fornecedor_comercial", T.StringType()),
                ]
            )
        )
        .parquet(config.get("dim_produto"))
        .withColumnsRenamed(
            {
                "codigo_fornecedor_principal_deposito": "cod_forn",
                "fornecedor_comercial": "forn_comercial",
            }
        )
        .filter(col("cod_forn") > 0)
        .dropDuplicates(["cod_forn"])
    )

    view_forn_pai = (
        forn_pai.join(forn_filho, "id_grupo_fornecedores_aporte_cab")
        .orderBy(forn_pai.data_hora_cadastro.desc())
        .dropDuplicates(["codigo_fornecedor"])
        .select(
            forn_filho.codigo_fornecedor.alias("cod_forn"),
            forn_pai.codigo_fornecedor_principal.alias("cod_forn_pai"),
        )
        .join(forn, F.expr("cod_forn_pai=forn_cd_fornecedor"))
        .withColumns(
            {
                "forn_nm_pai": strip_normalize("forn_nm_fantasia"),
                "cnpj_forn_pai": cnpj_normalize("forn_tn_cnpj"),
            }
        )
        .select("cod_forn", "cod_forn_pai", "forn_nm_pai", "cnpj_forn_pai")
    )

    return (
        forn.join(
            view_forn_pai, forn.forn_cd_fornecedor == view_forn_pai.cod_forn, "left"
        )
        .withColumns(
            {
                "forn_nm": strip_normalize("forn_nm_fantasia"),
                "cnpj_forn": cnpj_normalize("forn_tn_cnpj"),
            }
        )
        .select(
            col("forn_cd_fornecedor").alias("cod_forn"),
            "forn_nm",
            "cnpj_forn",
            F.coalesce(col("cod_forn_pai"), col("forn_cd_fornecedor")).alias(
                "cod_forn_pai"
            ),
            F.coalesce(col("forn_nm_pai"), col("forn_nm")).alias("forn_nm_pai"),
            F.coalesce(col("cnpj_forn_pai"), col("cnpj_forn")).alias("cnpj_forn_pai"),
        )
        .join(forn_comercial, "cod_forn", "left")
        .withColumn(
            "forn_comercial",
            strip_normalize(
                F.coalesce(col("forn_comercial"), F.lit("SEM FORNECEDOR PRINCIPAL"))
            ),
        )
    )


def view_coletas(
    spark: SparkSession, filter_year: int, config: dict[str, str]
) -> DataFrame:
    """Base das coletas por produto periodo e evento"""

    cab = spark.read.parquet(config.get("coleta_cab"))
    det = spark.read.parquet(config.get("coleta_det"))
    vol = spark.read.parquet(config.get("volume_tipo"))

    # NOTE: atributos
    custo_prod = col("rcde_vl_produto") - F.coalesce(col("rcde_vl_desconto"), F.lit(0))
    totais = col("rcde_qt_produto") * custo_prod
    empresa = F.when(col("rcde_cd_deposito") > 5, "EF").otherwise("PM")

    return (
        cab.where(F.year("rcca_dh_cadastro") == filter_year)
        .join(det, "id_recuperavel_coleta_cab")
        .join(vol, "id_recuperavel_tipo_volume")
        .groupBy(
            F.date_trunc("month", "rcca_dh_cadastro").alias("periodo"),
            empresa.alias("empresa"),
            col("rcde_cd_fornecedor_entrada").alias("cod_forn"),
            strip_normalize("rtv_desc_descricao").alias("evento"),
            col("rcde_cd_produto").alias("cod_prod"),
        )
        .agg(F.sum(totais).cast(T.DoubleType()).alias("perdas"))
    )


def view_credito(
    spark: SparkSession, year_filter: int, config: dict[str, str]
) -> DataFrame:
    """credito por fornecedor"""

    # tipos de negociacao
    ID_DESTINO = [127, 219, 44, 7, 112, 106, 282]

    neg = spark.read.parquet(config.get("negociacao"))
    deb = spark.read.parquet(config.get("debito"))
    pag = spark.read.parquet(config.get("pagamento"))
    sap = spark.read.parquet(config.get("dim_sap"))

    view_forn_sap = (
        spark.read.parquet(config.get("fornecedor"))
        .where(col("fsma_codigo_sap_master").isNotNull())
        .orderBy(col("xxxx_dh_cad").desc())
        .dropDuplicates(["fsma_codigo_sap_master"])
        .select(
            "forn_cd_fornecedor",
            col("fsma_codigo_sap_master").alias("fornecedor_principal_sap"),
        )
    )

    view_cred = (
        neg.join(deb, "id_negociacao_aporte")
        .join(pag, "id_debito_fornecedor")
        .filter(F.year(pag.data_hora_cadastro) == year_filter)
        .filter(col("id_destino_negociacao").isin(ID_DESTINO))
        .groupBy(
            F.date_trunc("month", pag.data_hora_cadastro).alias("periodo"),
            F.when(F.coalesce(col("codigo_empresa"), F.lit(1)) > 1, "EF")
            .otherwise("PM")
            .alias("empresa"),
            col("codigo_fornecedor").alias("cod_forn"),
        )
        .agg(F.sum(pag.valor).alias("credito"))
    )

    view_sap = (
        sap.filter(col("flag_credito") == 1)
        .filter(F.year("data_credito") == year_filter)
        .join(view_forn_sap, "fornecedor_principal_sap")
        .groupBy(
            F.date_trunc("month", col("data_credito")).alias("periodo"),
            col("flag_empresa").alias("empresa"),
            col("forn_cd_fornecedor").alias("cod_forn"),
        )
        .agg(F.sum("montante").alias("credito"))
    )

    return (
        view_cred.union(view_sap)
        .groupBy("periodo", "empresa", "cod_forn")
        .agg(F.sum("credito").cast(T.DoubleType()).alias("credito"))
    )


def main_view_ressarcimento(
    spark: SparkSession, year_filter: int, config: dict[str, str]
) -> DataFrame:
    """
    Resumo final base ressarcimento produto por ano
    """

    def strip(name, lit) -> Column:
        return strip_normalize(F.coalesce(col(name), F.lit(lit)))

    dim = (
        spark.read.schema(
            T.StructType(
                [
                    T.StructField("codigo_produto", T.LongType()),
                    T.StructField("nome_produto", T.StringType()),
                    T.StructField("fornecedor_comercial", T.StringType()),
                ]
            )
        )
        .parquet(config.get("dim_produto"))
        .withColumn("nome_produto", strip("nome_produto", "SEM NOME PROD"))
        .withColumn(
            "fornecedor_comercial",
            strip("fornecedor_comercial", "SEM FORNECEDOR PRINCIPAL"),
        )
    )

    forn = view_fornecedores(spark, config)
    coleta = view_coletas(spark, year_filter, config)
    credito = view_credito(spark, year_filter, config)

    cols_view = (
        coleta.alias("c")
        .join(dim.alias("d"), coleta.cod_prod == dim.codigo_produto)
        .join(forn, "cod_forn")
        .selectExpr(
            "c.*",
            "d.nome_produto",
            "forn_nm",
            "cnpj_forn_pai",
            "d.fornecedor_comercial as forn_comercial",
            "sum(perdas) over(partition by cnpj_forn_pai) as grupo_perda",
        )
    )

    creds_view = (
        credito.join(forn, "cod_forn")
        .groupBy(
            "periodo",
            "empresa",
            "cod_forn",
            "forn_nm",
            "cnpj_forn_pai",
            "forn_comercial",
        )
        .agg(F.sum("credito").alias("credito"))
    )

    creds_group = creds_view.groupBy("cnpj_forn_pai").agg(
        F.sum("credito").alias("credito")
    )

    total_ressarcimento = F.coalesce(
        (col("perdas") / F.ifnull(col("grupo_perda"), F.lit(0))) * col("credito"),
        col("credito"),
        F.lit(0),
    )

    rst_view = (
        cols_view.join(creds_group, "cnpj_forn_pai")
        .withColumn("ressarcimento", total_ressarcimento)
        .unionByName(
            creds_view.join(cols_view, "cnpj_forn_pai", "leftanti").withColumn(
                "ressarcimento", col("credito")
            ),
            allowMissingColumns=True,
        )
        .unionByName(
            cols_view.join(creds_group, "cnpj_forn_pai", "leftanti"),
            allowMissingColumns=True,
        )
        .drop("credito", "grupo_perda")
    )

    return rst_view.withColumns(
        {
            f.name: F.coalesce(col(f.name), F.lit(0))
            for f in rst_view.schema.fields
            if isinstance(f.dataType, (T.DoubleType, T.FloatType))
        }
    )
