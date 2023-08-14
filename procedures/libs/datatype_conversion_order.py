# from logger import Logger

# logger = Logger.logger
# ? Temporary disbale logger
#   to register this as a Snowpark UDF
# todo: write File Handler to Stage

# @Logger.log()
def get_highest_order_dtype(input_dtypes: list) -> str:
    """get_highest_order_dtype

    # !LOSSLESS DATATYPE CONVERSION PRINCIPLES
        : convert from one type to another without losing information, 
        with a strict number of conversion operations are allowed, that the datatype in the higher order in the hierachy should be chosen the present the lowers if exists.
        #*Precedence Hierachy
            variant < null
            string < double < float < int 
            string < array
            string < object
            string < bool

    Args:
       input_dtypes (list):  dtype instances detected from a field/column

    Returns:
        str: the highest order dtype to cast all values of this field to
    """

    precedence_hierarchy = [
        ("varchar", "double", "float", "int"),
        ("varchar", "object"),
        ("varchar", "array"),
        ("varchar", "boolean"),
    ]
    
    snowpark_type_mapping = dict(
        varchar = 'string'
    )
    
    # get distinct dtypes
    dtypes = set(input_dtypes)
    matched_prec_hierachy = list()
    highest_order_dtype = str()

    for input_dtype in dtypes:
        terminate = False
        for group_idx, group in enumerate(precedence_hierarchy):
            for prec_order, dtype in enumerate(group):
                if input_dtype.lower() in dtype:
                    matched_prec_hierachy.append((group_idx, prec_order, dtype))
                    terminate = True
                    break
            if terminate:
                break
                    
    # logger.debug('The precendence hierachy order of provided inputs are: %s', 
    #              ''.join([f'\n\t\t{str(x)}' for x in matched_prec_hierachy]))
    try:
        groups, prec_orders, dtypes = zip(*matched_prec_hierachy)
        distinct_groups = list(set(groups))
        # logger.debug('Provided inputs are in %s different group(s).', len(distinct_groups))
        match len(distinct_groups):
            case n if n > 1:
                highest_order_dtype = "varchar"
            case n if n == 1:
                cur_highest_pres = min(prec_orders)
                highest_order_dtype = precedence_hierarchy[distinct_groups[0]][
                    cur_highest_pres
                ]
        # logger.info('Highest precedence order dtype is %s', highest_order_dtype)
    except:
        # logger.warning('Provided inputs are not in any pre-defined precedence groups. Assigned Variant')
        highest_order_dtype = 'variant'
        
    # mapped with snowpark types
    highest_order_dtype = snowpark_type_mapping.get(highest_order_dtype,highest_order_dtype)
    return highest_order_dtype.upper()
