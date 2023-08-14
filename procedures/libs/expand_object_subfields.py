# import snowpark libs
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

from aivee_dev_tools.logger import Logger


logger = Logger.logger

config=dict(
    name=__file__,
    imports=[
        (
            "libs/datatype_conversion_order.py",
            "procedures.libs.datatype_conversion_order",
        )
    ]
)

def debug(session: snowpark.Session):
    sample_df = session.table('PUBLIC.RAW')
    result = expand_object_subfields(
        session=session,
        df=sample_df,
        obj_field='CONTACTS'
    )
    result.show()
    return result



@Logger.log(log_kwargs=["obj_field"])
def expand_object_subfields(
    session: snowpark.Session, df: snowpark.DataFrame, obj_field: str
) -> snowpark.DataFrame:
    """Expand the obj field's subfields to additional columns of the provided DataFrame

    Args:
        df (snowpark.DataFrame): a DataFrame containing the targeted field - OBJ type
        obj_field (str): the targeted field - OBJ type

    Returns:
        snowpark.DataFrame: The provided DataFrame with additional columns
    """
    
    key_dtype_pairs = (
        df.select(obj_field)
        .distinct()
        .join_table_function(
            "flatten", input=F.col(obj_field), mode=F.lit("OBJECT"), outer=F.lit(True)
        )
        .select("KEY", F.typeof("VALUE").as_("DTYPE"))
        .filter(F.col("KEY").isNotNull())
        .distinct()
        .group_by("KEY")
        .agg(F.array_agg(F.col("DTYPE")).as_("DTYPES"))
    )
    
    # ! the module below must be imported in the session since we register udf from another py module.
    # This can be done via imports key in the config block
    import procedures.libs.datatype_conversion_order as datatype_conversion_order
   
    get_highest_dtype_precedence = F.udf(
        datatype_conversion_order.get_highest_order_dtype, 
        return_type=T.StringType()
    )
    result = key_dtype_pairs.select(
        "KEY", get_highest_dtype_precedence("DTYPES").alias("HIGEST_PRECEDENCE_DTYPE")
    )

    converted_key_dtype_pairs = [row.as_dict() for row in result.collect()]
    logger.debug(
        "%s obj has %d subfields: %s",
        obj_field,
        len(converted_key_dtype_pairs),
        "".join([f"\n\t\t{str(x)}" for x in converted_key_dtype_pairs]),
    )
    col_subfield_names, col_subfield_values = zip(
        *[
            (
                f"{obj_field}__{pair['KEY']}",
                F.col(obj_field)[pair["KEY"]]._cast(pair["HIGEST_PRECEDENCE_DTYPE"]),
            )
            for pair in converted_key_dtype_pairs
        ]
    )
    df = df.with_columns(col_subfield_names, col_subfield_values)
    logger.debug("Expanded all subfields and drop the parent obj field.")

    return df
