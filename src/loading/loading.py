"""
Wikipedia Pageview Response Transformer

This module converts the nested response returned by the Wikimedia API
into a simple list of dictionaries that can be passed to the Bronze
loading layer.

The API response is designed for the API consumer, not for our data
pipeline. Before the data can be loaded into Spark, we need to transform
it into a consistent row-oriented structure.

The intended flow is:

    Wikimedia API
          ↓
    Nested API responses
          ↓
    flatten_results()
          ↓
    list[dict]
          ↓
    Bronze loader
          ↓
    Spark DataFrame
          ↓
    Bronze Delta table

This module is responsible only for transforming the API response.

It does NOT:

    - Call the API.
    - Write data to Unity Catalog.
    - Run Spark transformations.
    - Perform Bronze data-quality checks.
"""


# ============================================================================
# API Response Transformation
# ============================================================================

def flatten_results(
    results: dict,
) -> list[dict]:
    """
    Flatten nested Wikipedia API responses into row-level dictionaries.

    The API response contains multiple articles, with each article containing
    a list of daily pageview records.

    This function converts that nested structure into one dictionary per
    pageview record.

    Args:
        results:
            Dictionary containing API response data keyed by article title.

            Expected structure:

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
            A flattened list where each dictionary represents one pageview
            record.

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
    # Create the output collection
    # ------------------------------------------------------------------------
    #
    # Each API pageview record will become one dictionary in this list.
    #
    # We use a list because the next layer of the pipeline will convert these
    # records into a Spark DataFrame.
    # ------------------------------------------------------------------------

    flattened_data: list[dict] = []

    # ------------------------------------------------------------------------
    # Process each article
    # ------------------------------------------------------------------------
    #
    # The outer dictionary is keyed by article title.
    #
    # Example:
    #
    #     "Minecraft" -> API response
    #     "Fortnite"  -> API response
    #
    # We process each article independently so that all of its pageview
    # records can be extracted.
    # ------------------------------------------------------------------------

    for article_title, response_data in results.items():

        # --------------------------------------------------------------------
        # Extract the pageview items
        # --------------------------------------------------------------------
        #
        # The Wikimedia API stores the daily pageview records inside the
        # "items" field.
        #
        # Using .get("items", []) means a missing "items" field results in an
        # empty list instead of raising a KeyError.
        #
        # This allows the transformation to continue processing other
        # articles if one API response is incomplete.
        # --------------------------------------------------------------------

        for item in response_data.get("items", []):

            # ----------------------------------------------------------------
            # Build one normalized record
            # ----------------------------------------------------------------
            #
            # Instead of carrying the entire API response into Bronze, we
            # explicitly select the fields our pipeline needs.
            #
            # This gives us a predictable internal schema and prevents
            # unnecessary API-specific fields from leaking into downstream
            # layers.
            # ----------------------------------------------------------------

            record = {
                "project": item.get("project"),
                "article": item.get("article"),
                "granularity": item.get("granularity"),
                "timestamp": item.get("timestamp"),
                "views": item.get("views"),
            }

            # ---------------------------------------------------------------
            # Add the normalized record to the output
            # ---------------------------------------------------------------

            flattened_data.append(record)

    # ------------------------------------------------------------------------
    # Return flattened records
    # ------------------------------------------------------------------------
    #
    # The result is now ready to be passed to the Bronze loading layer.
    #
    # Example:
    #
    #     flattened_data
    #         ↓
    #     write_bronze()
    #         ↓
    #     Spark DataFrame
    # ------------------------------------------------------------------------

    return flattened_data
