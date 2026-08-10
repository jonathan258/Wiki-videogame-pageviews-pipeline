def flatten_results(results: dict) -> list[dict]:
    """
    Flatten the nested results dictionary into a list of dictionaries.

    Args:
        results: A dictionary containing
        API response data keyed by article title.

    Returns:
        A list of dictionaries, each representing a flattened record.
    """
    flattened_data = []

    for article_title, response_data in results.items():
        for item in response_data.get("items", []):
            record = {
                "project": item.get("project"),
                "article": item.get("article"),
                "granularity": item.get("granularity"),
                "timestamp": item.get("timestamp"),
                "views": item.get("views"),
            }

            flattened_data.append(record)

    return flattened_data
