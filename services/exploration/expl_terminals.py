import pandas as pd
def dataframe_terminal_restrictions(allowed_terminals, forbidden_terminals):
    """
    Retrieve a dataframe with the ter.

    :param barge_id:
    :return:
    """

    terminals_restriction = {}

    for terminal in sorted(allowed_terminals):
        if terminal[:5] == 'VNVUT':
            terminals_restriction[terminal] = [12, 25, False]
        else:
            terminals_restriction[terminal] = [12, 20, False]

    for terminal in sorted(forbidden_terminals):
        if terminal[:5] == 'VNVUT':
            terminals_restriction[terminal] = [12, 25, True]
        else:
            terminals_restriction[terminal] = [12, 20, True]


    return pd.DataFrame(terminals_restriction, index=['waiting (h)', 'moves (h)', 'forbidden']).T