from unittest.mock import Mock, patch

import requests

from ingestion.ingestion import fetch_api_data

'''
What is happening in this unit test
1. Create fake response
        |
2. Give fake response fake JSON
        |
3. Tell requests.get to return fake response
        |
4. Call YOUR function
        |
5. Check what YOUR function returned
'''


@patch("ingestion.ingestion.requests.get")
def test_fetch_api_data_success(mock_get):
    mock_response = Mock()
    
    # Make fake JSON data that looks like the Wikipedia API response.
    mock_response.json.return_value = {
        "items": [
            {
                "project": "en.wikipedia",
                "article": "The_Legend_of_Zelda",
                "granularity": "daily",
                "timestamp": "2022010100",
            }
        ]
    }
    
    mock_response.raise_for_status.return_value = None
    
    # When requests.get is called, return our fake response.
    mock_get.return_value = mock_response
    
    result = fetch_api_data(
        "The_Legend_of_Zelda",
        "20220101",
        "20220131"
        )

    # The function should return the fake JSON data.
    assert result == mock_response.json.return_value
    mock_get.assert_called_once()
    

@patch("ingestion.ingestion.requests.get")
def test_fetch_api_data_failure(mock_get):
    mock_get.side_effect = requests.exceptions.RequestException(
        "API request failed"
        )
    result = fetch_api_data(
        "The_Legend_of_Zelda",
        "20220101",
        "20220131"
        )
    assert result is None
    mock_get.assert_called_once()
