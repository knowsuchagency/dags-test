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
        # NOTE: this is for initial prototyping in my own corporate account (Stephan Fitzpatrick)
        "knowsuchagency": {
            "mwaa": {
                "subnet_ids": ["subnet-03edd4c423858445d", "subnet-074244c237be0796e"],
                "vpc": "vpc-0f89d7c72ed3f7600",
            }
        },
    }
)
