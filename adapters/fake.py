@'
def search(query=None, location=None, limit=12):
    """
    Fake adapter used for smoke tests. Returns a small list of job dicts.
    """
    return [
        {"title": "FAKE Job 1", "company": "Acme", "location": location or "Remote", "url": "https://example.com/1", "snippet": "Test job 1"},
        {"title": "FAKE Job 2", "company": "Acme", "location": location or "Remote", "url": "https://example.com/2", "snippet": "Test job 2"},
    ][:limit]
'@ | Set-Content -Path .\adapters\fake.py -Force -Encoding UTF8

Write-Output "Created adapters\fake.py"
