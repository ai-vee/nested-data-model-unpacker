# import snowpark libs
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

# import project-level libs
from aivee_dev_tools.logger import Logger

config = dict(
    name=__file__,
    imports=[
        (
            "libs/expand_object_subfields.py",
            "procedures.libs.expand_object_subfields",
        ),
        (
            "libs/flatten_array.py",
            "procedures.libs.flatten_array",
        )
    ]
)

def debug(session: snowpark.Session):
    result = unpack_json_document(
        session=session,
        input_relation='PUBLIC.RAW',
        output_schema='OUTPUT',
        output_table_prefix='TEMP_',
        output_table_suffix='_STRUCTURED'
    )
    # result.show()
    return result

def unpack_json_document(
    session: snowpark.Session,
    input_relation: str,
    output_schema: str,
    detect_dtype_from_first=50,
    max_recursive_calls: int = 100,
    output_table_prefix: str = "",
    output_table_suffix: str = "",
    exclude: list = [],
) -> dict:
    """Generate a data model comprising of 1:1 and 1:M relationship objects encapsulated in a mutilayered nested JSON document. It will define schema on the fly, hence, it is schema-agnostic

    Args:
        session (snowpark.Session): explicitly define if run locally. Passed by the current session if calling as a sproc in Snowflake environment.
        input_relation: full relational address of the input table
        output_schema: schema where output tables will be written to
        detect_dtype_from_first (int, optional): the first n instances used to detect datatype. Defaults to 50.
        output_table_prefix: prefix added to each output table name
        output_table_suffix: suffix added to each output table name
        exclude: exclude not unpacking these fields/cols

    Raises:
        TypeError: if a targeted field to unpack does not have a unique dtype of either ARRAY or OBJECT

    Returns:
        dict: a dictionary of DataFrame's names and corresponding DataFrame Objects.
    """

    logger = Logger.logger

    df = session.table(input_relation)

    # * HELPER FUNCS
    from procedures.libs.expand_object_subfields import expand_object_subfields
    from procedures.libs.flatten_array import flatten_array

    processed_fields = set()

    @Logger.log(log_kwargs=["parent_path"])
    def recursive_unpacker(df: snowpark.DataFrame, parent_path="", counter=0) -> None:
        """recursively process an inital dataframe's unprocessed columns.
        If a column is an array, each row's combound values will be flatten into multiple rows.
            If after flatten, each row is an Object
                a new df will be created from this column
                
        If a column is an object, each subfield will be expanded as an additonal column
        
        Each column's row once presented as a single value. This column will be added to the global processed_fields logs
        
        Recursive func terminates when there is no more columns to processed

        Args:
            df (snowpark.DataFrame): a DataFrame obj containing any struct fields
            detect_dtype_from_first (int, optional): the first n instances used to detect datatype. Defaults to 50.
            parent_path (str, optional): use as prefix of result DataFrames and table names. Defaults to ''.
            counter (int, optional): counter of recursive calls. Defaults to 0.

        Raises:
            TypeError: if a targeted field to unpack does not have a unique dtype of either ARRAY or OBJECT

        """

        counter += 1
        logger.debug(
            "Func (%s) recursive call starts with %s-row Dataframe. \n\tGlobal processed_fields = %s",
            counter,
            df.count(),
            processed_fields,
        )
        exception_list = exclude + ["SHA256"]
        cur_unprocessed_fields = set(
            [
                col
                for col in df.columns
                if all(x not in col for x in exception_list)
                and col not in processed_fields
            ]
        )
        if len(cur_unprocessed_fields) == 0 or counter == max_recursive_calls:
            logger.warning("Terminated at %s recursive calls", counter)
            return

        new_dfs = dict()
        for cur_field in cur_unprocessed_fields:
            # cast curfield to variant to conduct type detection
            logger.debug("Current field: %s", cur_field)
            target = df.select(
                F.col(cur_field).cast(T.VariantType()).alias(cur_field)
            ).distinct()
            # target.show()
            to_unpack_cur_field_dtypes = (
                target.limit(detect_dtype_from_first)
                .select(F.typeof(F.col(cur_field)).as_("TYPEOF"))
                .distinct()
                .filter(F.col("TYPEOF").isin(F.lit("ARRAY"), F.lit("OBJECT")))
            ).collect()
            distinct_to_unpack_type_count = len(to_unpack_cur_field_dtypes)
            if distinct_to_unpack_type_count > 1:
                raise TypeError(
                    '"%s" does not have a unique dtype of either ARRAY or OBJECT.'
                )
            elif distinct_to_unpack_type_count == 1:
                dtype = to_unpack_cur_field_dtypes[0].__getitem__("TYPEOF")
                logger.debug("%s is an %s", cur_field, dtype)
                match dtype:
                    case "ARRAY":
                        hash_field = f"{cur_field}_SHA256"
                        df = df.with_column(
                            hash_field,
                            F.sha2(F.col(cur_field).cast(T.StringType()), 256),
                        )
                        new_df = flatten_array(session, df, array_field=cur_field)
                        new_dfs[cur_field] = new_df
                    case "OBJECT":
                        df = expand_object_subfields(session, df, obj_field=cur_field)
                        processed_fields.add(cur_field)
                        logger.debug(
                            '"%s" reach ends of path. "%s" > processed_fields',
                            cur_field,
                            cur_field,
                        )
                # df.show()
                df = df.drop(cur_field)
            else:
                processed_fields.add(cur_field)
                logger.debug(
                    '"%s" reach ends of path. "%s" > processed_fields',
                    cur_field,
                    cur_field,
                )

        globals()[
            f"INTERNAL_RESULT_{output_table_prefix}{parent_path}{output_table_suffix}"
        ] = df
        logger.info(
            "Write %s to Globals",
            f"INTERNAL_RESULT_{output_table_prefix}{parent_path}{output_table_suffix}",
        )
        recursive_unpacker(df, parent_path=parent_path, counter=counter)

        for name, df in new_dfs.items():
            logger.debug("Process new df %s", name)
            recursive_unpacker(df, parent_path=name, counter=counter)

    # * CALL MAIN FUNC
    recursive_unpacker(df, parent_path=input_relation.split(".")[-1])
    results = {
        k.replace("INTERNAL_RESULT_", ""): v
        for k, v in globals().items()
        if "INTERNAL_RESULT" in k
    }
    result_df: snowpark.DataFrame
    for name, result_df in results.items():
        logger.info("Results: %s", name)
        result_df.show()
        result_df.write.save_as_table(f"{output_schema}.{name}", mode="overwrite")
    
    logs = df.select('INGESTION_CTRL', 'DATASET_SERVICE', F.expr('current_timestamp as FLATTEN_AT'))
    return results
