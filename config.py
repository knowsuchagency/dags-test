import os

from box import Box

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


CONFIG = Box(
    {
        "dev": {
            "mwaa": {
                "subnet_ids": ["subnet-02936ce771f7216e2", "subnet-04768ae7fc3e9ae32"],
                "vpc": "vpc-0b18a53b9d7fa154a",
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
