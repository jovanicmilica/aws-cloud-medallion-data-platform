import os
import json
import requests

def lambda_handler(event, context):
    webhook = os.environ["DISCORD_WEBHOOK"]

    try:
        formatted_event = json.dumps(event, indent=2)

        response = requests.post(
            webhook,
            json={
                "content": f"Pipeline alert!\n```json\n{formatted_event}\n```"
            },
            timeout=5
        )

        response.raise_for_status()

        return {
            "statusCode": 200,
            "body": "Sent"
        }

    except Exception as e:
        print(f"Failed to send Discord message: {e}")
        return {
            "statusCode": 500,
            "body": str(e)
        }