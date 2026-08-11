import requests
import logging
from urllib.parse import quote

logger = logging.getLogger(__name__)

# This function asks Wikipedia for page view numbers for one article.
# It returns the API result or None if something goes wrong.


def fetch_api_data(
    article_title: str,
    start_date: str,
    end_date: str,
) -> dict | None:
    """
    Fetch Wikipedia pageview data for an article.

    Args:
        article_title: Wikipedia article title.
        start_date: Start date in YYYYMMDD format.
        end_date: End date in YYYYMMDD format.

    Returns:
        Parsed JSON response as a dictionary, or None if the request fails.
    """
    # Make the article title safe for the URL.
    encoded_title = quote(article_title, safe="")
    # Build the full API URL we need to call.
    url = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
        f"per-article/en.wikipedia.org/all-access/user/"
        f"{encoded_title}/daily/{start_date}/{end_date}"
        )

    headers = {
        "User-Agent": (
            "wiki-videogame-pageviews-pipeline/1.0 "
            "(https://github.com/jonathan258/Wiki-videogame-pageviews-pipeline)"
        )
    }

    try:
        # Ask the web for the pageview data.
        response = requests.get(
            url,
            headers=headers,
            timeout=10,
        )

        response.raise_for_status()

        # If the request works, return the JSON data.
        return response.json()
    except requests.exceptions.RequestException as http_err:
        # If the request fails, log the error and return None.
        logger.error("HTTP error occurred: %s", http_err)
        return None


article_titles = [
    "The_Legend_of_Zelda",
    "Super_Mario_Bros.",
    "Minecraft",
    "Fortnite_(video_game)",
    "Grand_Theft_Auto_V",
    "Call_of_Duty:_Modern_Warfare_(2019_video_game)",
    "Among_Us_(video_game)",
    "Cyberpunk_2077",
    "The_Witcher_3:_Wild_Hunt",
    "Red_Dead_Redemption_2",
]
start_date = "20260101"
end_date = "20260131"

if __name__ == "__main__":
    results = {}
    for title in article_titles:
        data = fetch_api_data(title, start_date, end_date)
        if data is not None:
            results[title] = data
        else:
            logger.warning("Failed to fetch data for article: %s", title)
