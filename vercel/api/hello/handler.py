def handler(request):
    """
    A minimal Vercel Python function.
    This will respond at /api/hello
    """
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": '{"message":"Hello from plain Python on Vercel!"}'
    }
