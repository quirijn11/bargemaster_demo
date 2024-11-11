from enum import Enum
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from data.service_database import load_datatable_from_db
from polyline import decode
from services.backend.barge_route_graphs import RouteCalculator


class FinancialItemTypes(Enum):
    """
    Enum class to define the types of financial items
    """

    SAILING = 'SAILING'
    STATIONARY = 'STATIONARY'
    OPERATIONS = 'OPERATIONS'
    MAINTENANCE = 'MAINTENANCE'
    EXPLOITATION = 'EXPLOITATION'
    PLANNING = 'PLANNING'
    TRANSPORT = 'TRANSPORT'
    ADMINISTRATION = 'ADMINISTRATION'

class FinancialItemCategory(Enum):
    """
    Enum class to define the categories of financial items

    """
    AGENCY = 'AGENCY'
    BERTHING = 'BERTHING'
    CIF = 'CIF'
    CONTRIBUTION = 'CONTRIBUTION'
    CUSTOMS = 'CUSTOMS'
    FIXED_COST = 'FIXED_COST'
    FOB = 'FOB'
    FUEL = 'FUEL'
    FICY = 'FI/CY'
    INSURANCE = 'INSURANCE'
    STEVEDORING = 'STEVEDORING'
    TOLL = 'TOLL'
    # TRANSPORT = 'TRANSPORT'
    STOWAGE = 'STOWAGE'
    PILOTAGE = 'PILOTAGE'
    PORT = 'PORT'
    WAGES = 'WAGES'
    BROKERAGE = 'BROKERAGE'
    GENERAL = 'GENERAL'

class FinancialItemSpecification(Enum):
    """
    Enum class to define the specification of financial items
    """

    KM = 'KM'
    UNIT = 'UNIT'

    LITRE = 'LITRE'
    TON = 'TON'
    VOLUME = 'VOLUME'

    CALL = 'CALL'
    VOYAGE = 'VOYAGE'

    TEU = 'TEU'
    FEET = 'FEET'

    HOUR = 'HOUR'
    DAY = 'DAY'
    MONTH = 'MONTH'

class OperatorGroup(Enum):
    """
    Enum class to define the operator group

    """

    BARGE = 'BARGE'
    ICD = 'ICD'
    LOGISTIC = 'LOGISTIC'
    PLATFORM = 'PLATFORM'
    PORT = 'PORT'
    SHIPPER = 'SHIPPER'
    TERMINAL = 'TERMINAL'

class FinancialTransaction:
    """
    Class to define the financial transaction

    """

    def __init__(self, data):
        self.data = data
        self.terminalOperators = ["VNVUTDGML"]
        self.terminals = load_datatable_from_db("terminals")
        self.barges = load_datatable_from_db("barges")
        self.operators = load_datatable_from_db("operator")
        self.tariffs = load_datatable_from_db("tariffs")
        self.voyages = self.split_twin_calls()

    def split_twin_calls(self):
        """
        Method to get the financial transaction

        """

        twin_call = self.data.copy()

        twin_call['total_discharge'] = twin_call['discharge_20'] + twin_call[
            'discharge_40'] * 2 + twin_call['discharge_45'] * 2
        twin_call['total_load'] = twin_call['load_20'] + twin_call[
            'load_40'] * 2 + twin_call['load_45'] * 2

        # Retrieve all the voyages and split them up
        import_export_indices = twin_call[(twin_call['voyage_number_import'].notna()) &
                                                      (twin_call['voyage_number_export'].notna())
                                                      ].index

        new_rows = []

        # if location is in terminalOperators, create a record of import voyage with clearing the discharge numbers and export voyage with loading the discharge numbers
        for index in import_export_indices:
            original_row = twin_call.loc[[index]]
            import_voyage = original_row.copy()
            export_voyage = original_row.copy()

            if original_row['terminal_id'].values[0] in self.terminalOperators:
                # empty the colums of import_voyage and export_voyage and load discharge numbers
                import_voyage[
                    'voyage_number_export'] = np.nan  # if terminal, then import means load numbers and export means discharge numbers
                import_voyage[['discharge_20', 'discharge_40', 'discharge_45']] = 0
                export_voyage['voyage_number_import'] = np.nan
                export_voyage[['load_20', 'load_40', 'load_45']] = 0

            else:
                # empty the colums of import_voyage and export_voyage and load discharge numbers
                export_voyage[
                    'voyage_number_export'] = np.nan  # if not terminal, then export means load numbers and import means discharge numbers
                export_voyage[['discharge_20', 'discharge_40', 'discharge_45']] = 0
                import_voyage['voyage_number_import'] = np.nan
                import_voyage[['load_20', 'load_40', 'load_45']] = 0

            new_rows.append(import_voyage)
            new_rows.append(export_voyage)

        # Drop the original rows with import and export voyages
        twin_call.drop(import_export_indices, inplace=True)

        # Concatenate the new rows with the original DataFrame
        twin_call = pd.concat([twin_call] + new_rows, ignore_index=True)

        check_import_export_indices = twin_call[(twin_call['voyage_number_import'].notna()) &
                                                            (twin_call['voyage_number_export'].notna())
                                                            ].index

        assert twin_call.shape[0] == self.data.shape[0] + len(
            import_export_indices), "Financial transaction is not equal to the data"

        assert check_import_export_indices.empty, "Import and export voyages are not splitted properly"


        twin_call['voyage_number'] = twin_call['voyage_number_import'].fillna(twin_call['voyage_number_export'])
        twin_call.sort_values(['voyage_number', 'start_date_time'], inplace=True)
        twin_call = twin_call[
            ['voyage_number', 'barge_call_sign', 'terminal_id', 'start_date_time',
             'end_date_time', 'total_discharge', 'total_load']].reset_index(drop=True)


        return twin_call

    def add_navigation_helper(self):
        """
        Add the navigation helper to the dataframe
        :return:
        """

        self.voyages["route"] = self.voyages['terminal_id'] + self.voyages['terminal_id'].shift(-1)
        self.terminals['terminal_cd'] = self.terminals['unlocode'] + self.terminals['terminal_code']
        calc_distance = RouteCalculator()
        calc_distance.create_graph()

        for index, row in self.voyages.iterrows():
            if index + 1 < self.voyages.shape[0]:
                if self.voyages.loc[index, "voyage_number"] == self.voyages.loc[index + 1, "voyage_number"]:
                    route_distance = calc_distance.calculate_shortest_path(row['route'][0:9], row['route'][9:18])
                    self.voyages.loc[index, "km_to_next_stop"] = round(calc_distance.shortest_path_length / 1000, 2)

            self.voyages.loc[index, "area_authority"] = self.terminals.loc[self.terminals['terminal_cd'] == row['terminal_id'], 'place'].values[0]

    def add_parties(self):
        """
        Add the parties to the dataframe
        :return:
        """

        map_barge_operator = (self.barges.merge(self.operators, left_on='operator_id', right_on='id', how='left').
                              set_index('call_sign')['abbreviation'].to_dict())
        map_terminal_operator = (self.terminals.merge(self.operators, left_on='operator_id', right_on='id', how='left')
                                 .set_index('terminal_cd')['abbreviation'].to_dict())


        self.voyages['barge_operator'] = self.voyages['barge_call_sign'].map(map_barge_operator)

        self.voyages['icd_operator'] = self.voyages['terminal_id'].map(map_terminal_operator)

        self.voyages['terminal_operator'] = (self.voyages['terminal_id']
                                             .apply(lambda x: x if x in self.terminalOperators else np.nan))


        self.voyages = self.voyages[['voyage_number', 'barge_call_sign', 'terminal_id',
                                     'start_date_time', 'end_date_time',
                                     'barge_operator', 'icd_operator', 'terminal_operator',
                                      'total_discharge', 'total_load','km_to_next_stop', 'area_authority']]

    def get_financial_items(self):
        """
        Method to get the financial items

        """

        unique_voyages = self.voyages['voyage_number'].unique()
        unique_debtors = self.tariffs['debtor'].unique()
        financial_transactions = {
            'voyage_number': [],
            'debtor': [],
            'creditor': [],
            'activity': [],
            'service': [],
            'unit': [],
            'amount': [],
            'tariff': [],
            'currency': [],
            'price': []
        }

        for voyage in unique_voyages:
            voy_finan = self.voyages[self.voyages['voyage_number'] == voyage]
            icd_list = list(voy_finan[voy_finan['icd_operator'].notna()]['icd_operator'].unique())
            barge_list = list(voy_finan[voy_finan['barge_operator'].notna()]['barge_operator'].unique())
            terminal_list = list(voy_finan[voy_finan['terminal_operator'].notna()]['terminal_operator'].unique())
            area_authority_list = list(voy_finan[voy_finan['area_authority'].notna()]['area_authority'].unique())


            for debtor in unique_debtors:

                if debtor == OperatorGroup.SHIPPER.value:
                    run_through_debtor_tariffs = self.tariffs[self.tariffs['debtor'] == OperatorGroup.SHIPPER.value]
                    for t_label, t_row in run_through_debtor_tariffs.iterrows():
                        if t_row['creditor'] == OperatorGroup.ICD.value:
                            for icd in icd_list:
                                amount = int(voy_finan[voy_finan['icd_operator'] == icd]['total_load'].sum())
                                amount += int(voy_finan[voy_finan['icd_operator'] == icd]['total_discharge'].sum())

                                financial_transactions['voyage_number'].append(voyage)
                                financial_transactions['debtor'].append('CMA')
                                financial_transactions['creditor'].append(icd)
                                financial_transactions['tariff'].append(t_row['tariff'])
                                financial_transactions['activity'].append(t_row['activity'])
                                financial_transactions['service'].append(t_row['service'])
                                financial_transactions['unit'].append(t_row['unit'])
                                financial_transactions['amount'].append(amount)
                                financial_transactions['currency'].append(t_row['currency'])
                                financial_transactions['price'].append(t_row['tariff'] * amount)

                if debtor == OperatorGroup.ICD.value:
                    run_through_debtor_tariffs = self.tariffs[self.tariffs['debtor'] == OperatorGroup.ICD.value]
                    for t_label, t_row in run_through_debtor_tariffs.iterrows():
                        # There can be different ICD's in the same voyage
                        for icd in icd_list:

                            if t_row['unit'] == FinancialItemSpecification.TEU.value:
                                amount = int(voy_finan[voy_finan['icd_operator'] == icd]['total_load'].sum())
                                amount += int(voy_finan[voy_finan['icd_operator'] == icd]['total_discharge'].sum())
                            else:
                                print(f"Found a unit for ICD {unit} that isn't supported yet")
                                break

                            if t_row['creditor'] == OperatorGroup.PLATFORM.value:
                                financial_transactions['voyage_number'].append(voyage)
                                financial_transactions['debtor'].append(icd)
                                financial_transactions['creditor'].append('RIVA')
                                financial_transactions['tariff'].append(t_row['tariff'])
                                financial_transactions['activity'].append(t_row['activity'])
                                financial_transactions['service'].append(t_row['service'])
                                financial_transactions['unit'].append(t_row['unit'])
                                financial_transactions['amount'].append(amount)
                                financial_transactions['currency'].append(t_row['currency'])
                                financial_transactions['price'].append(t_row['tariff'] * amount)
                                continue

                            if icd != voy_finan['barge_operator'].values[0] and t_row['creditor'] == OperatorGroup.BARGE.value:
                                financial_transactions['voyage_number'].append(voyage)
                                financial_transactions['debtor'].append(icd)
                                financial_transactions['creditor'].append(voy_finan['barge_operator'].values[0])
                                financial_transactions['tariff'].append(t_row['tariff'])
                                financial_transactions['activity'].append(t_row['activity'])
                                financial_transactions['service'].append(t_row['service'])
                                financial_transactions['unit'].append(t_row['unit'])
                                financial_transactions['amount'].append(amount)
                                financial_transactions['currency'].append(t_row['currency'])
                                financial_transactions['price'].append(t_row['tariff'] * amount)

                            if not t_row['creditor']:
                                financial_transactions['voyage_number'].append(voyage)
                                financial_transactions['debtor'].append(icd)
                                financial_transactions['creditor'].append(t_row['creditor'])
                                financial_transactions['tariff'].append(t_row['tariff'])
                                financial_transactions['activity'].append(t_row['activity'])
                                financial_transactions['service'].append(t_row['service'])
                                financial_transactions['unit'].append(t_row['unit'])
                                financial_transactions['amount'].append(amount)
                                financial_transactions['currency'].append(t_row['currency'])
                                financial_transactions['price'].append(t_row['tariff'] * amount)

                if debtor == OperatorGroup.BARGE.value:
                    run_through_debtor_tariffs = self.tariffs[self.tariffs['debtor'] == OperatorGroup.BARGE.value]
                    for t_label, t_row in run_through_debtor_tariffs.iterrows():

                        if t_row['creditor'] == OperatorGroup.ICD.value:
                            for icd in icd_list:
                                if icd == voy_finan['barge_operator'].values[0]:
                                    continue
                                if t_row['unit'] == FinancialItemSpecification.TEU.value:
                                    amount = int(voy_finan[voy_finan['icd_operator'] == icd]['total_load'].sum())
                                    amount += int(voy_finan[voy_finan['icd_operator'] == icd]['total_discharge'].sum())
                                elif t_row['unit'] == FinancialItemSpecification.CALL.value:
                                    amount = len(voy_finan[voy_finan['icd_operator'] == icd])
                                else:
                                    print(f"Found a unit for ICD {unit} that isn't supported yet")
                                    break

                                financial_transactions['voyage_number'].append(voyage)
                                financial_transactions['debtor'].append(voy_finan['barge_operator'].values[0])
                                financial_transactions['creditor'].append(icd)
                                financial_transactions['tariff'].append(t_row['tariff'])
                                financial_transactions['activity'].append(t_row['activity'])
                                financial_transactions['service'].append(t_row['service'])
                                financial_transactions['unit'].append(t_row['unit'])
                                financial_transactions['amount'].append(amount)
                                financial_transactions['currency'].append(t_row['currency'])
                                financial_transactions['price'].append(t_row['tariff'] * amount)

                        if t_row['creditor'] == OperatorGroup.TERMINAL.value:
                            for terminal in terminal_list:
                                if t_row['unit'] == FinancialItemSpecification.TEU.value:
                                    amount = int(voy_finan[voy_finan['terminal_operator'] == icd]['total_load'].sum())
                                    amount += int(voy_finan[voy_finan['terminal_operator'] == icd]['total_discharge'].sum())
                                elif t_row['unit'] == FinancialItemSpecification.CALL.value:
                                    amount = len(voy_finan[voy_finan['terminal_operator'] == terminal])
                                else:
                                    print(f"Found a unit for terminal {unit} that isn't supported yet")
                                    break

                                financial_transactions['voyage_number'].append(voyage)
                                financial_transactions['debtor'].append(voy_finan['barge_operator'].values[0])
                                financial_transactions['creditor'].append(terminal)
                                financial_transactions['tariff'].append(t_row['tariff'])
                                financial_transactions['activity'].append(t_row['activity'])
                                financial_transactions['service'].append(t_row['service'])
                                financial_transactions['unit'].append(t_row['unit'])
                                financial_transactions['amount'].append(amount)
                                financial_transactions['currency'].append(t_row['currency'])
                                financial_transactions['price'].append(t_row['tariff'] * amount)

                        if not t_row['creditor']:
                            financial_transactions['voyage_number'].append(voyage)
                            financial_transactions['debtor'].append(voy_finan['barge_operator'].values[0])
                            financial_transactions['creditor'].append(t_row['creditor'])
                            financial_transactions['tariff'].append(t_row['tariff'])
                            financial_transactions['activity'].append(t_row['activity'])
                            financial_transactions['service'].append(t_row['service'])
                            financial_transactions['unit'].append(t_row['unit'])
                            financial_transactions['currency'].append(t_row['currency'])

                            if (t_row['unit'] == FinancialItemSpecification.KM.value and
                                    t_row['service'] == FinancialItemCategory.FUEL.value):
                                amount = voy_finan['km_to_next_stop'].sum()

                            elif (t_row['unit'] == FinancialItemSpecification.CALL.value and
                                  t_row['service'] == FinancialItemCategory.FUEL.value):
                                amount = len(voy_finan)

                            elif (t_row['unit'] == FinancialItemSpecification.VOYAGE.value and
                                  t_row['service'] == FinancialItemCategory.GENERAL.value):
                                amount = 1  # because we calculate the tariff per voyage

                            elif (t_row['unit'] == FinancialItemSpecification.CALL.value and
                                  t_row['service'] == FinancialItemCategory.TOLL.value):
                                amount = len(area_authority_list)
                                if amount > 0:
                                    amount -= 1

                            financial_transactions['amount'].append(amount)
                            financial_transactions['price'].append(round(t_row['tariff'] * amount, 2))

        return financial_transactions

