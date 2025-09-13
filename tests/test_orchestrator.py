import unittest
from unittest.mock import patch, MagicMock
import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.orchestrator import run

class TestOrchestrator(unittest.TestCase):

    def run_test_with_config(self, run_config, mock_config, mock_e, mock_d, mock_y):
        # Configure the mock Config object
        mock_config_instance = mock_config.return_value
        def get_side_effect(key, default=None):
            if key == "run_steps":
                return run_config["run_steps"]
            return {
                "DB_DOC_LIST_TABLE": "DocumentList",
                "DB_FINANCIAL_DATA_TABLE": "FinancialData",
                "DB_STANDARDIZED_TABLE": "StandardizedData",
                "DB_PATH": "dummy.db"
            }.get(key, default)
        mock_config_instance.get.side_effect = get_side_effect

        # Mock the instances of the classes
        mock_edinet_instance = MagicMock()
        mock_e.Edinet.return_value = mock_edinet_instance
        
        mock_data_instance = MagicMock()
        mock_d.data.return_value = mock_data_instance

        # Call the run function
        run(edinet=mock_edinet_instance, data=mock_data_instance)

        return mock_edinet_instance, mock_data_instance, mock_y


    @patch('src.orchestrator.y')
    @patch('src.orchestrator.d')
    @patch('src.orchestrator.e')
    @patch('src.orchestrator.Config')
    def test_run_with_get_documents_true(self, mock_config, mock_e, mock_d, mock_y):
        """
        Tests that the orchestrator calls get_All_documents_withMetadata when get_documents is true.
        """
        run_config = {
            "run_steps": {
                "get_documents": True,
                "download_documents": False,
                "standardize_data": False,
                "generate_financial_ratios": False,
                "aggregate_ratios": False,
                "update_stock_prices": False
            }
        }
        
        mock_edinet_instance, mock_data_instance, mock_y_instance = self.run_test_with_config(run_config, mock_config, mock_e, mock_d, mock_y)

        # Assert that the correct methods were called
        mock_edinet_instance.get_All_documents_withMetadata.assert_called_once()
        mock_edinet_instance.downloadDocs.assert_not_called()
        mock_data_instance.copy_table_to_Standard.assert_not_called()
        mock_data_instance.Generate_Financial_Ratios.assert_not_called()
        mock_data_instance.Generate_Aggregated_Ratios.assert_not_called()
        mock_y_instance.update_all_stock_prices.assert_not_called()

    @patch('src.orchestrator.y')
    @patch('src.orchestrator.d')
    @patch('src.orchestrator.e')
    @patch('src.orchestrator.Config')
    def test_run_with_download_documents_true(self, mock_config, mock_e, mock_d, mock_y):
        """
        Tests that the orchestrator calls downloadDocs when download_documents is true.
        """
        run_config = {
            "run_steps": {
                "get_documents": False,
                "download_documents": True,
                "standardize_data": False,
                "generate_financial_ratios": False,
                "aggregate_ratios": False,
                "update_stock_prices": False
            }
        }
        
        mock_edinet_instance, mock_data_instance, mock_y_instance = self.run_test_with_config(run_config, mock_config, mock_e, mock_d, mock_y)

        mock_edinet_instance.get_All_documents_withMetadata.assert_not_called()
        mock_edinet_instance.downloadDocs.assert_called_once()
        mock_data_instance.copy_table_to_Standard.assert_not_called()
        mock_data_instance.Generate_Financial_Ratios.assert_not_called()
        mock_data_instance.Generate_Aggregated_Ratios.assert_not_called()
        mock_y_instance.update_all_stock_prices.assert_not_called()

    @patch('src.orchestrator.y')
    @patch('src.orchestrator.d')
    @patch('src.orchestrator.e')
    @patch('src.orchestrator.Config')
    def test_run_with_standardize_data_true(self, mock_config, mock_e, mock_d, mock_y):
        """
        Tests that the orchestrator calls copy_table_to_Standard when standardize_data is true.
        """
        run_config = {
            "run_steps": {
                "get_documents": False,
                "download_documents": False,
                "standardize_data": True,
                "generate_financial_ratios": False,
                "aggregate_ratios": False,
                "update_stock_prices": False
            }
        }
        
        mock_edinet_instance, mock_data_instance, mock_y_instance = self.run_test_with_config(run_config, mock_config, mock_e, mock_d, mock_y)

        mock_edinet_instance.get_All_documents_withMetadata.assert_not_called()
        mock_edinet_instance.downloadDocs.assert_not_called()
        mock_data_instance.copy_table_to_Standard.assert_called_once()
        mock_data_instance.Generate_Financial_Ratios.assert_not_called()
        mock_data_instance.Generate_Aggregated_Ratios.assert_not_called()
        mock_y_instance.update_all_stock_prices.assert_not_called()

    @patch('src.orchestrator.y')
    @patch('src.orchestrator.d')
    @patch('src.orchestrator.e')
    @patch('src.orchestrator.Config')
    def test_run_with_generate_financial_ratios_true(self, mock_config, mock_e, mock_d, mock_y):
        """
        Tests that the orchestrator calls Generate_Financial_Ratios when generate_financial_ratios is true.
        """
        run_config = {
            "run_steps": {
                "get_documents": False,
                "download_documents": False,
                "standardize_data": False,
                "generate_financial_ratios": True,
                "aggregate_ratios": False,
                "update_stock_prices": False
            }
        }
        
        mock_edinet_instance, mock_data_instance, mock_y_instance = self.run_test_with_config(run_config, mock_config, mock_e, mock_d, mock_y)

        mock_edinet_instance.get_All_documents_withMetadata.assert_not_called()
        mock_edinet_instance.downloadDocs.assert_not_called()
        mock_data_instance.copy_table_to_Standard.assert_not_called()
        mock_data_instance.Generate_Financial_Ratios.assert_called_once()
        mock_data_instance.Generate_Aggregated_Ratios.assert_not_called()
        mock_y_instance.update_all_stock_prices.assert_not_called()

    @patch('src.orchestrator.y')
    @patch('src.orchestrator.d')
    @patch('src.orchestrator.e')
    @patch('src.orchestrator.Config')
    def test_run_with_aggregate_ratios_true(self, mock_config, mock_e, mock_d, mock_y):
        """
        Tests that the orchestrator calls Generate_Aggregated_Ratios when aggregate_ratios is true.
        """
        run_config = {
            "run_steps": {
                "get_documents": False,
                "download_documents": False,
                "standardize_data": False,
                "generate_financial_ratios": False,
                "aggregate_ratios": True,
                "update_stock_prices": False
            }
        }
        
        mock_edinet_instance, mock_data_instance, mock_y_instance = self.run_test_with_config(run_config, mock_config, mock_e, mock_d, mock_y)

        mock_edinet_instance.get_All_documents_withMetadata.assert_not_called()
        mock_edinet_instance.downloadDocs.assert_not_called()
        mock_data_instance.copy_table_to_Standard.assert_not_called()
        mock_data_instance.Generate_Financial_Ratios.assert_not_called()
        mock_data_instance.Generate_Aggregated_Ratios.assert_called_once()
        mock_y_instance.update_all_stock_prices.assert_not_called()

    @patch('src.orchestrator.y')
    @patch('src.orchestrator.d')
    @patch('src.orchestrator.e')
    @patch('src.orchestrator.Config')
    def test_run_with_update_stock_prices_true(self, mock_config, mock_e, mock_d, mock_y):
        """
        Tests that the orchestrator calls update_all_stock_prices when update_stock_prices is true.
        """
        run_config = {
            "run_steps": {
                "get_documents": False,
                "download_documents": False,
                "standardize_data": False,
                "generate_financial_ratios": False,
                "aggregate_ratios": False,
                "update_stock_prices": True
            }
        }
        
        mock_edinet_instance, mock_data_instance, mock_y_instance = self.run_test_with_config(run_config, mock_config, mock_e, mock_d, mock_y)

        mock_edinet_instance.get_All_documents_withMetadata.assert_not_called()
        mock_edinet_instance.downloadDocs.assert_not_called()
        mock_data_instance.copy_table_to_Standard.assert_not_called()
        mock_data_instance.Generate_Financial_Ratios.assert_not_called()
        mock_data_instance.Generate_Aggregated_Ratios.assert_not_called()
        mock_y_instance.update_all_stock_prices.assert_called_once()

    @patch('src.orchestrator.y')
    @patch('src.orchestrator.d')
    @patch('src.orchestrator.e')
    @patch('src.orchestrator.Config')
    def test_run_with_all_steps_false(self, mock_config, mock_e, mock_d, mock_y):
        """
        Tests that the orchestrator calls no functions when all steps are false.
        """
        run_config = {
            "run_steps": {
                "get_documents": False,
                "download_documents": False,
                "standardize_data": False,
                "generate_financial_ratios": False,
                "aggregate_ratios": False,
                "update_stock_prices": False
            }
        }
        
        mock_edinet_instance, mock_data_instance, mock_y_instance = self.run_test_with_config(run_config, mock_config, mock_e, mock_d, mock_y)

        mock_edinet_instance.get_All_documents_withMetadata.assert_not_called()
        mock_edinet_instance.downloadDocs.assert_not_called()
        mock_data_instance.copy_table_to_Standard.assert_not_called()
        mock_data_instance.Generate_Financial_Ratios.assert_not_called()
        mock_data_instance.Generate_Aggregated_Ratios.assert_not_called()
        mock_y_instance.update_all_stock_prices.assert_not_called()

    @patch('src.orchestrator.y')
    @patch('src.orchestrator.d')
    @patch('src.orchestrator.e')
    @patch('src.orchestrator.Config')
    def test_run_with_all_steps_true(self, mock_config, mock_e, mock_d, mock_y):
        """
        Tests that the orchestrator calls all functions when all steps are true.
        """
        run_config = {
            "run_steps": {
                "get_documents": True,
                "download_documents": True,
                "standardize_data": True,
                "generate_financial_ratios": True,
                "aggregate_ratios": True,
                "update_stock_prices": True
            }
        }
        
        mock_edinet_instance, mock_data_instance, mock_y_instance = self.run_test_with_config(run_config, mock_config, mock_e, mock_d, mock_y)

        mock_edinet_instance.get_All_documents_withMetadata.assert_called_once()
        mock_edinet_instance.downloadDocs.assert_called_once()
        mock_data_instance.copy_table_to_Standard.assert_called_once()
        mock_data_instance.Generate_Financial_Ratios.assert_called_once()
        mock_data_instance.Generate_Aggregated_Ratios.assert_called_once()
        mock_y_instance.update_all_stock_prices.assert_called_once()

if __name__ == '__main__':
    unittest.main()