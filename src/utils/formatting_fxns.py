#Functions used for conditional formatting

import pandas as pd
import logging

#---------------------------------------------------------------------------------------------

def false_bold_condform(val): #style map
    if val == False:
        bold = 'bold'
    else:
        bold = ''
    return f'font-weight: {bold}'

#---------------------------------------------------------------------------------------------

def in_list_bold_condform(val): 
    """
    Function Actions:
    - Style mapping: Conditional formatting applied to a df.
    - Value to change to Bold (if in false list). 
    """
    #3. 'these get used for conditional_formatting
    if val in false_list:
        bold = 'bold'
    else:
        bold = ''
    return 'font-weight: %s' % bold
#---------------------------------------------------------------------------------------------

def false_red_condform(val): #style map
    if val == False:
        colour = 'red'
    elif val == True:
        colour = 'green'
    else:
        colour = ''
    return f'color: {colour}'

#---------------------------------------------------------------------------------------------

def substr_in_col_bool(df, col1, col2): 
    """
    Function Actions:
    - Style map. 
    - Make global lists of the rows that are true or false.
    """
    global true_list
    global false_list

    true_list = []
    false_list = []
    #run through each row of the df and check add False rows into a list, and same for True.
    #place into either true or false lists.
    for i in df.index:
        if df[col1][i] == False:
            false_list.append(df[col2][i])
        elif df[col1][i] == True:
            true_list.append(df[col2][i])

    return true_list, false_list

#---------------------------------------------------------------------------------------------
##Currently not used anywhere obvious?
# def test_false_true(df, col, accept_list, outcome):
#     """
#     Function Actions:
#     - Style map. 
#     """
#     mask = df[col].str.contains('|'.join(accept_list))==outcome #outcome = True or False
#     result = df[mask]
#     return(result)


#---------------------------------------------------------------------------------------------

def bool_condform(val): 
    """
    Function Actions:
    - Style mapping: Conditional formatting applied to a df.
    - Value to change font colour to green (if in true list) or red (if in false list). 
    """
    if val in false_list:
        color = 'red'
    elif val in true_list:
        color = 'green'
    else:
        color = ''
    return 'color: %s' % color

#---------------------------------------------------------------------------------------------
def identify_empty_cols(df):
    """  Check a dataframe to see if all rows within a column are populated.
     Log a warning if any empty cells are identified. """
    
    #create a list of all column names
    col_list = df.columns.values.tolist()

    #for each column, check if there are any empty rows (we are not expecting any)
    
    for col in col_list:
        #list of indices where there are empty cells
        empty_indexes_list = df[df[col].isnull()].index.tolist()
        if len(empty_indexes_list) != 0:
            logging.warning(f'For dataframe {df}, one or more rows have been identified to be empty in column {col}. We would expect all rows to filled. Please review. ')  
        else:
            pass  

#---------------------------------------------------------------------------------------------
def slice_dict_list(d, k:int):
    """
    Actions: 
    - filters/slices out a dictionary's values list sizes to only include up to the nth index.
    - e.g. {A: [a,b,c,d], E:[e,f,g,h], I:[j,k,l,m]}

    Inputs:
    - d= dictionary where keys have listed values
    - k= the nth position of a values list, e.g. 2

    Outputs:
    - same dictionary but with reduced value list sizes.
    - e.g. {A: [a,b], E:[e,f], I:[j,k]}
        
    """
    #if d is a dictionary, return but with listed values up to the nth index
    if isinstance(d, dict):
        return {key: slice_dict_list(value, k) for key, value in d.items()}
    #if d is a list, slice this  up to the nth index
    elif isinstance(d, list):
        return d[:k]
    else:
        return d