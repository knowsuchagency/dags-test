import os

from box import Box

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


CONFIG = Box(
    {
        "dev": {
            "mwaa": {
                "subnet_ids": ["subnet-05381beba1da37295", "subnet-0e88f922959e7bb28"],
                "vpc": "vpc-0d819d4ed2f6cf33f",
            }
        },
    }
)
