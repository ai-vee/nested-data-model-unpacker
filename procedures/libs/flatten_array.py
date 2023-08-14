# import snowpark libs
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

# import module-level libs
from aivee_dev_tools.logger import Logger

logger = Logger.logger

config = dict(name=__file__, imports=[])

@Logger.log(log_kwargs=['array_field'])
def flatten_array(session: snowpark.Session, df: snowpark.DataFrame, array_field: str) -> snowpark.DataFrame:
    """Explode a distinct Array instances of the specified field, with each Array row's combound values into multiple rows.

    Args:
        df (snowpark.DataFrame): a DataFrame containing at least 2 fields: a sha hash of the array value, and the array field
        array_field (str): targeted array field to explode

    Returns:
        snowpark.DataFrame: new DataFrame as a result of the flatten array and the corresponding hash field
    """
    
    DROPPED_COLS = ['SEQ','KEY','PATH','INDEX','THIS'] #Extra cols we dont need after flatten
    logger.debug('Flattening array "%s".............', array_field)
    hash_field = f"{array_field}_SHA256"
    unique_arr_instances = (df.select(hash_field, array_field).distinct()) #to reduce the no. instances to flatten
    flatten_result = (unique_arr_instances
                                .join_table_function('flatten',
                                                        input=F.col(array_field),
                                                        mode=F.lit('ARRAY'), 
                                                        outer=F.lit(True))
                                .drop(*DROPPED_COLS, array_field)
                                .with_column_renamed('VALUE', array_field))
    logger.debug('"%s" after flatten generated a new df of %d rows', array_field, flatten_result.count())
    return flatten_result
