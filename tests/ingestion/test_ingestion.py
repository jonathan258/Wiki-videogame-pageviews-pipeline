"""
Unit Tests: Wikipedia API Client

These tests verify the behavior of fetch_api_data() without making real
HTTP requests to the Wikimedia API.

Instead of contacting the real API, we replace requests.get() with a
Mock object.

This makes the tests:

    - Fast
    - Repeatable
    - Independent of the internet
    - Safe to run in CI/CD
    - Focused only on our application's behavior

The tests cover two important scenarios:

    1. Successful API request
    2. Failed API request

Testing the failure path is especially important because API calls can fail
because of network problems, timeouts, server errors, or other HTTP issues.
"""


from unittest.mock import Mock, patch

import requests

from ingestion.ingestion import fetch_api_data


# ============================================================================
# Successful API Request
# ============================================================================

@patch("ingestion.ingestion.requests.get")
def test_fetch_api_data_success(
    mock_get,
):
    """
    Test that fetch_api_data() returns the API JSON when the request succeeds.

    The real requests.get() function is replaced with a Mock object.

    Test flow:

        1. Create a fake HTTP response.
        2. Give the fake response fake JSON data.
        3. Tell requests.get() to return the fake response.
        4. Call our fetch_api_data() function.
        5. Verify that our function returned the expected data.
        6. Verify that requests.get() was called once.
    """

    # ------------------------------------------------------------------------
    # Step 1: Create a fake HTTP response
    # ------------------------------------------------------------------------
    #
    # Mock allows us to simulate the behavior of requests.Response without
    # actually making an HTTP request.
    # ------------------------------------------------------------------------

    mock_response = Mock()

    # ------------------------------------------------------------------------
    # Step 2: Create fake API response data
    # ------------------------------------------------------------------------
    #
    # This represents the kind of JSON structure we expect from Wikimedia.
    #
    # We don't need a complete API response for this unit test.
    # We only need enough data to verify that our function correctly returns
    # the response.
    # ------------------------------------------------------------------------

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

    # ------------------------------------------------------------------------
    # Step 3: Simulate a successful HTTP request
    # ------------------------------------------------------------------------
    #
    # In the real requests library, raise_for_status() raises an exception
    # when the HTTP response represents an error.
    #
    # Returning None simulates a successful HTTP response.
    # ------------------------------------------------------------------------

    mock_response.raise_for_status.return_value = None

    # ------------------------------------------------------------------------
    # Step 4: Tell requests.get() to return our fake response
    # ------------------------------------------------------------------------
    #
    # Because @patch replaced requests.get with a Mock object,
    # mock_get controls what the function receives from the HTTP request.
    #
    # No network request will actually happen.
    # ------------------------------------------------------------------------

    mock_get.return_value = mock_response

    # ------------------------------------------------------------------------
    # Step 5: Call the function we are testing
    # ------------------------------------------------------------------------
    #
    # We use realistic arguments so the test behaves like an actual
    # application call.
    # ------------------------------------------------------------------------

    result = fetch_api_data(
        "The_Legend_of_Zelda",
        "20220101",
        "20220131",
    )

    # ------------------------------------------------------------------------
    # Step 6: Verify the returned data
    # ------------------------------------------------------------------------
    #
    # The function should return the JSON provided by our fake API response.
    #
    # This verifies that:
    #
    #     requests.get()
    #          ↓
    #     response.json()
    #          ↓
    #     fetch_api_data()
    #
    # correctly produces the expected result.
    # ------------------------------------------------------------------------

    assert result == mock_response.json.return_value

    # ------------------------------------------------------------------------
    # Step 7: Verify the HTTP request happened exactly once
    # ------------------------------------------------------------------------
    #
    # This protects against accidentally making multiple API requests inside
    # the function.
    # ------------------------------------------------------------------------

    mock_get.assert_called_once()


# ============================================================================
# Failed API Request
# ============================================================================

@patch("ingestion.ingestion.requests.get")
def test_fetch_api_data_failure(
    mock_get,
):
    """
    Test that fetch_api_data() returns None when the API request fails.

    Instead of returning a response, the mocked requests.get() raises a
    RequestException.

    This simulates network/API failures without requiring the real API to
    actually fail.
    """

    # ------------------------------------------------------------------------
    # Step 1: Make the fake HTTP request raise an exception
    # ------------------------------------------------------------------------
    #
    # side_effect tells Mock to raise the specified exception whenever
    # requests.get() is called.
    # ------------------------------------------------------------------------

    mock_get.side_effect = requests.exceptions.RequestException(
        "API request failed"
    )

    # ------------------------------------------------------------------------
    # Step 2: Call the function being tested
    # ------------------------------------------------------------------------

    result = fetch_api_data(
        "The_Legend_of_Zelda",
        "20220101",
        "20220131",
    )

    # ------------------------------------------------------------------------
    # Step 3: Verify the error-handling behavior
    # ------------------------------------------------------------------------
    #
    # fetch_api_data() is designed to catch RequestException and return None
    # rather than allowing the exception to crash the ingestion pipeline.
    # ------------------------------------------------------------------------

    assert result is None

    # ------------------------------------------------------------------------
    # Step 4: Verify that the API was attempted exactly once
    # ------------------------------------------------------------------------

    mock_get.assert_called_once()
