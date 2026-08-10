from transform.flatten import flatten_results


def test_flatten_results():
    fake_results = {
        "The_Legend_of_Zelda": {
            "items": [
                {
                    "project": "en.wikipedia",
                    "article": "The_Legend_of_Zelda",
                    "granularity": "daily",
                    "timestamp": "20220101",
                    "views": 500,
                },
                {
                    "project": "en.wikipedia",
                    "article": "The_Legend_of_Zelda",
                    "granularity": "daily",
                    "timestamp": "20220102",
                    "views": 620,
                },
            ]
        }
    }

    result = flatten_results(fake_results)

    assert result == [
        {
            "project": "en.wikipedia",
            "article": "The_Legend_of_Zelda",
            "granularity": "daily",
            "timestamp": "20220101",
            "views": 500,
        },
        {
            "project": "en.wikipedia",
            "article": "The_Legend_of_Zelda",
            "granularity": "daily",
            "timestamp": "20220102",
            "views": 620,
        },
    ]
