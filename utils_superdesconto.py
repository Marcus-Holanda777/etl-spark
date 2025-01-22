from pyspark.sql.functions import col
from pyspark.sql import (
    DataFrame, 
    Column,
    SparkSession
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import datetime, timedelta


cols_rename = [
    "filial", "cod_prod", 
    "periodo", "etiqueta", 
    "perc_dsc_cupom", "venda", 
    "venda_desconto"
]

cols_cosmos = [
    "MVVC_CD_FILIAL_MOV",
    "MVVP_NR_PRD",
    "MVVC_DT_MOV",
    "NUMERO_AUTORIZ_PAGUEMENOS",
    "MVVP_PR_DSC_ITE",
    "MVVP_VL_PRE_VDA",
    "MVVP_VL_PRD_VEN",
]

cols_pre_venda = [
    "VC_CD_FILIAL",
    "VD_CD_PRODUTO",
    "VC_DH_VENDA",
    "VD_COD_ETIQUETA_ULCH",
    "VD_PERC_DESCONTO",
    "VD_VL_PRODUTO",
    "VD_VL_PRODUTO_COM_DESCONTO",
]

cols_autorizador = [
    "ulch_sq_autorizacao",
    "ulch_preco_venda",
    "ulch_percentual_desconto",
    "ulch_fl_tipo_produto",
    "ulch_cd_barras",
    "ulch_fl_situacao",
    "ulch_sq_produto"
]

cols_produto = [
    "ulch_sq_produto",
    "xxxx_dh_cad",
    "ulch_lote",
    "ulch_dt_vencimento",
    "ulch_sq_produto"
]


def etiqueta(colname: str) -> Column:
    return F.lpad(F.trim(colname), 30, "0").cast(T.StringType())


def list_files(
    path: str, 
    start: datetime, 
    end: datetime,
    config: dict[str, str]
):
    days = (end - start).days + 1
    for day in range(days):
        dt = start + timedelta(day)
        yield f"{config.get('bucket_uc')}/{path}/{dt:%Y/%m/%d}.parquet"
     

def view_pre_venda(
    spark: SparkSession,
    path: str, 
    start: datetime, 
    end: datetime, 
    columns: list[str],
    config: dict[str, str]
) -> DataFrame:
    
    col_etiqueta = columns[3]
    files_path = list(list_files(path, start, end, config))

    return (
        spark.read.parquet(*files_path)
        .select(columns)
        .withColumn(col_etiqueta, etiqueta(col_etiqueta))
        .withColumnsRenamed(dict(zip(columns, cols_rename)))
    )


def view_cupom(
    spark: SparkSession, 
    start: datetime, 
    end: datetime,
    config: dict[str, str]
) -> DataFrame:
    
    windows = (
        Window.partitionBy("etiqueta")
         .orderBy(col("venda_desconto").desc())
    )

    return (
        view_pre_venda(spark, "COSMOSMOV", start, end, cols_cosmos, config)
        .union(view_pre_venda(spark, "PRE_VENDA", start, end, cols_pre_venda, config))
        .withColumn("id", F.row_number().over(windows))
        .filter(col("id") == 1)
        .drop("id")
    )


def view_autorizador(
    spark: SparkSession, 
    config: dict[str, str]
) -> DataFrame:
    
    file = config.get('autorizacao')
    
    return (
        spark.read.parquet(file)
        .select(cols_autorizador)
        .filter(col("ulch_fl_situacao") == "F")
        .withColumn("ulch_cd_barras", etiqueta("ulch_cd_barras"))
        .withColumn("ulch_percentual_desconto", F.coalesce("ulch_percentual_desconto", F.lit(0)))
        .dropDuplicates(["ulch_cd_barras"])
    )


def view_produto(
    spark: SparkSession,
    config: dict[str, str]
) -> DataFrame:
    
    file = config.get('produto.parquet')
    
    return (
        spark.read.parquet(file)
        .select(cols_produto)
        .withColumn("ulch_lote", F.upper(F.trim("ulch_lote")))
        .dropDuplicates(["ulch_sq_produto"])
    )