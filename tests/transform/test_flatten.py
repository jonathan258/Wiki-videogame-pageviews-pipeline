"""
Unit Tests: Wikipedia Pageview Transformation

These tests verify the behavior of flatten_results().

The transformation function does not communicate with an external system,
so there is no need to use mocks.

Instead, we provide a small, controlled example of the nested API response
and verify that the function converts it into the expected flat structure.

The test covers:

    Nested API response
            ↓
       flatten_results()
            ↓
        list[dict]
            ↓
      Expected records
"""


from transform.flatten import flatten_results


# ============================================================================
# Flatten Results
# ============================================================================

def test_flatten_results():
    """
    Test that a nested API response is correctly flattened.

    The fake response contains two daily pageview records for one article.

    Expected behavior:

        1 nested article response
                    ↓
        2 pageview items
                    ↓
        2 flat dictionaries
    """

    # ------------------------------------------------------------------------
    # Step 1: Create fake API response data
    # ------------------------------------------------------------------------
    #
    # This represents the structure returned by the Wikimedia API.
    #
    # We intentionally keep the dataset very small because this is a unit
    # test. We only need enough data to prove that the transformation works.
    # ------------------------------------------------------------------------

    fake_results = {
        "The_Legend_of_Zelda": {
            "items": [
                {
                    "project": "en.wikipedia",
                    "article": "The_Legend_of_Zelda",
                    "granularity": "daily",
                    "timestamp": "2022010100",
                    "views": 500,
                },
                {
                    "project": "en.wikipedia",
                    "article": "The_Legend_of_Zelda",
                    "granularity": "daily",
                    "timestamp": "2022010200",
                    "views": 620,
                },
            ]
        }
    }

    # ------------------------------------------------------------------------
    # Step 2: Run the transformation
    # ------------------------------------------------------------------------
    #
    # The nested API response is passed into the function we are testing.
    # ------------------------------------------------------------------------

    result = flatten_results(
        fake_results
    )

    # ------------------------------------------------------------------------
    # Step 3: Verify the transformed result
    # ------------------------------------------------------------------------
    #
    # The function should:
    #
    #     - Remove the nested "items" structure.
    #     - Create one dictionary per pageview record.
    #     - Preserve the values from the API response.
    #
    # The resulting list should therefore contain exactly two records.
    # ------------------------------------------------------------------------

    assert result == [
        {
            "project": "en.wikipedia",
            "article": "The_Legend_of_Zelda",
            "granularity": "daily",
            "timestamp": "2022010100",
            "views": 500,
        },
        {
            "project": "en.wikipedia",
            "article": "The_Legend_of_Zelda",
            "granularity": "daily",
            "timestamp": "2022010200",
            "views": 620,
        },
    ]
