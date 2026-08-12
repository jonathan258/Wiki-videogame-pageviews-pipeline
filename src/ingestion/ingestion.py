"""
Wikipedia Pageview API Client

This module is responsible for communicating with the Wikimedia Pageviews API.

The main responsibility of this module is API ingestion:

    1. Build the API request.
    2. Encode article titles safely for use in the URL.
    3. Send the HTTP request.
    4. Validate the HTTP response.
    5. Return the API response as Python data.
    6. Log failures without crashing the entire ingestion process.

This module does NOT perform Spark transformations or data-quality checks.
Keeping API ingestion separate makes the code easier to test, reuse, and
maintain.
"""

import logging
from urllib.parse import quote

import requests


# ============================================================================
# Logging
# ============================================================================
#
# Create a module-level logger instead of using print() for operational
# messages.
#
# The application or orchestration layer can configure logging later, while
# this module simply sends messages to the logger.
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================
#
# These are the articles we currently want to ingest.
#
# Eventually, this configuration should be moved into a shared configuration
# file so that ingestion, validation, and orchestration all use the same
# source of truth.
# ============================================================================

ARTICLE_TITLES = [
    "The_Legend_of_Zelda",
    "Super_Mario_Bros.",
    "Minecraft",
    "Fortnite_(video_game)",
    "Grand_Theft_Auto_V",
    "Call_of_Duty:*Modern_Warfare*(2019_video_game)",
    "Among_Us_(video_game)",
    "Cyberpunk_2077",
    "The_Witcher_3:_Wild_Hunt",
    "Red_Dead_Redemption_2",
]

START_DATE = "20260101"
END_DATE = "20260131"


# Wikimedia recommends identifying API clients with a descriptive User-Agent.
#
# Including project information makes the request easier for API operators
# to identify if there is ever an issue with the requests.
HEADERS = {
    "User-Agent": (
        "wiki-videogame-pageviews-pipeline/1.0 "
        "(https://github.com/jonathan258/Wiki-videogame-pageviews-pipeline)"
    )
}


# ============================================================================
# API Ingestion
# ============================================================================

def fetch_api_data(
    article_title: str,
    start_date: str,
    end_date: str,
) -> dict | None:
    """
    Fetch Wikipedia pageview data for one article.

    This function is intentionally responsible only for making the API
    request and returning the response.

    Transformation, validation, storage, and orchestration should happen
    elsewhere.

    Args:
        article_title:
            Wikipedia article title.

        start_date:
            Start date in YYYYMMDD format.

        end_date:
            End date in YYYYMMDD format.

    Returns:
        Parsed JSON response as a dictionary if the request succeeds.

        None if the API request fails.
    """

    # ------------------------------------------------------------------------
    # Encode the article title
    # ------------------------------------------------------------------------
    #
    # Wikipedia article titles can contain characters such as spaces,
    # parentheses, colons, or other characters that have special meaning
    # inside URLs.
    #
    # quote() converts the title into a URL-safe representation.
    # ------------------------------------------------------------------------

    encoded_title = quote(
        article_title,
        safe="",
    )

    # ------------------------------------------------------------------------
    # Build the API URL
    # ------------------------------------------------------------------------
    #
    # Keeping URL construction separate from the HTTP request makes the
    # function easier to understand and easier to debug.
    # ------------------------------------------------------------------------

    url = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
        "per-article/en.wikipedia.org/all-access/user/"
        f"{encoded_title}/daily/{start_date}/{end_date}"
    )

    # ------------------------------------------------------------------------
    # Send API request
    # ------------------------------------------------------------------------
    #
    # timeout prevents the pipeline from waiting indefinitely if the API
    # becomes unavailable or stops responding.
    # ------------------------------------------------------------------------

    try:

        response = requests.get(
            url,
            headers=HEADERS,
            timeout=10,
        )

        # raise_for_status() converts HTTP errors such as 404 or 500 into
        # exceptions that can be handled by our error handling below.
        response.raise_for_status()

        # --------------------------------------------------------------------
        # Parse successful response
        # --------------------------------------------------------------------
        #
        # The Wikimedia API returns JSON. response.json() converts that JSON
        # into normal Python objects, allowing the rest of the pipeline to
        # process the response.
        # --------------------------------------------------------------------

        return response.json()

    except requests.exceptions.RequestException as http_err:

        # --------------------------------------------------------------------
        # Handle HTTP/network failures
        # --------------------------------------------------------------------
        #
        # We log the error instead of crashing the entire ingestion process.
        #
        # The caller receives None and can decide whether the failed article
        # should be retried, skipped, quarantined, or cause the pipeline to
        # fail.
        # --------------------------------------------------------------------

        logger.error(
            "HTTP error while fetching article '%s': %s",
            article_title,
            http_err,
        )

        return None


# ============================================================================
# Ingestion Runner
# ============================================================================

def fetch_all_articles(
    article_titles: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, dict]:
    """
    Fetch pageview data for all requested Wikipedia articles.

    The function loops through the configured articles and collects only
    successful API responses.

    Args:
        article_titles:
            List of Wikipedia article titles to fetch.

        start_date:
            Start date in YYYYMMDD format.

        end_date:
            End date in YYYYMMDD format.

    Returns:
        Dictionary where:

            key   = article title
            value = API response
    """

    results: dict[str, dict] = {}

    # ------------------------------------------------------------------------
    # Process each article independently
    # ------------------------------------------------------------------------
    #
    # Each article is fetched separately because the Wikimedia API endpoint
    # is based on the individual article title.
    #
    # If one article fails, we continue processing the remaining articles.
    # This prevents one bad API request from automatically losing the entire
    # ingestion run.
    # ------------------------------------------------------------------------

    for title in article_titles:

        logger.info(
            "Fetching pageview data for article: %s",
            title,
        )

        data = fetch_api_data(
            article_title=title,
            start_date=start_date,
            end_date=end_date,
        )

        # --------------------------------------------------------------------
        # Store successful response
        # --------------------------------------------------------------------

        if data is not None:

            results[title] = data

        else:

            # Keep a warning in the logs so the failed article can be
            # investigated later by the pipeline or data-quality process.
            logger.warning(
                "Failed to fetch data for article: %s",
                title,
            )
    return results


# ============================================================================
# Local Execution
# ============================================================================
#
# This block allows the module to be executed directly during development.
#
# Importing the module from another part of the application will NOT execute
# this code because it is protected by the __name__ check.
# ============================================================================

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    results = fetch_all_articles(
        article_titles=ARTICLE_TITLES,
        start_date=START_DATE,
        end_date=END_DATE,
    )

    logger.info(
        "Successfully fetched %d of %d articles.",
        len(results),
        len(ARTICLE_TITLES),
    )
