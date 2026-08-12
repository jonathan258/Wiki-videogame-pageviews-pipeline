"""
Wikipedia Pageview Response Transformer

This module transforms the nested response returned by the Wikimedia
Pageviews API into a simple, row-oriented list of dictionaries.

The API response is nested because each article contains its own collection
of daily pageview records. Spark works much more naturally with tabular,
row-oriented data, so this transformation prepares the API response for
the Bronze ingestion layer.

Pipeline flow:

    Wikimedia API
          ↓
    Nested API response
          ↓
    flatten_results()
          ↓
    list[dict]
          ↓
    Bronze DataFrame
          ↓
    Bronze Delta table

This module is responsible only for transformation.

It does NOT:

    - Make API requests.
    - Create Spark DataFrames.
    - Write to Unity Catalog.
    - Perform data-quality checks.
"""


# ============================================================================
# API Response Transformation
# ============================================================================

def flatten_results(
    results: dict,
) -> list[dict]:
    """
    Flatten nested Wikimedia API responses into row-level dictionaries.

    Each daily pageview record from the API becomes one dictionary in the
    returned list.

    Args:
        results:
            Dictionary containing API responses keyed by article title.

            Example:

                {
                    "Minecraft": {
                        "items": [
                            {...},
                            {...},
                        ]
                    },
                    "Fortnite_(video_game)": {
                        "items": [
                            {...},
                            {...},
                        ]
                    }
                }

    Returns:
        list[dict]:
            A flattened collection where each dictionary represents one
            daily pageview record.

            Example:

                [
                    {
                        "project": "en.wikipedia.org",
                        "article": "Minecraft",
                        "granularity": "daily",
                        "timestamp": "2026010100",
                        "views": 12345,
                    }
                ]
    """

    # ------------------------------------------------------------------------
    # Create the output list
    # ------------------------------------------------------------------------
    #
    # Every pageview item returned by the API will become one dictionary
    # inside this list.
    #
    # We use a list because this structure can be passed directly to the
    # Bronze loader, which will convert it into a Spark DataFrame.
    # ------------------------------------------------------------------------

    flattened_data: list[dict] = []

    # ------------------------------------------------------------------------
    # Iterate through each article response
    # ------------------------------------------------------------------------
    #
    # The outer dictionary is organized by article title.
    #
    # Example:
    #
    #     "Minecraft" -> API response
    #     "Fortnite"  -> API response
    #
    # Each article response contains an "items" collection containing its
    # individual pageview records.
    # ------------------------------------------------------------------------

    for article_title, response_data in results.items():

        # --------------------------------------------------------------------
        # Extract each pageview record
        # --------------------------------------------------------------------
        #
        # The Wikimedia API stores the daily pageview records inside the
        # "items" field.
        #
        # Using .get("items", []) protects us from a missing "items" field.
        # Instead of raising a KeyError, the article simply produces no
        # records and the transformation can continue with the remaining
        # articles.
        # --------------------------------------------------------------------

        for item in response_data.get("items", []):

            # ----------------------------------------------------------------
            # Create a normalized record
            # ----------------------------------------------------------------
            #
            # We intentionally select only the fields our pipeline currently
            # needs instead of carrying the entire API response downstream.
            #
            # This creates a stable internal structure that the Bronze loader
            # and downstream Spark transformations can work with.
            # ----------------------------------------------------------------

            record = {
                "project": item.get("project"),
                "article": item.get("article"),
                "granularity": item.get("granularity"),
                "timestamp": item.get("timestamp"),
                "views": item.get("views"),
            }

            # Add the normalized record to our output collection.
            flattened_data.append(record)

    # ------------------------------------------------------------------------
    # Return the transformed records
    # ------------------------------------------------------------------------
    #
    # At this point the nested API structure has been converted into a flat
    # list of dictionaries.
    #
    # The next stage of the pipeline can now pass this result to the Bronze
    # loader.
    # ------------------------------------------------------------------------

    return flattened_data
