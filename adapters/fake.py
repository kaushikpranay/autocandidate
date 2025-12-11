def search(query=None, location=None, limit=12):
    return [
        {"title": "FAKE Job 1", "company": "Acme", "location": location or "Remote", "url": "https://example.com/1", "snippet": "Test job 1"},
    ][:limit]
