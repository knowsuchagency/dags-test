from typing import Literal

Environment = Literal[
    "dev",
    "stage",
    "prod",
    # TODO: remove once no longer necessary
    "knowsuchagency",
]

AirflowVersion = Literal[
    "2.0.2",
    "2.2.2",
]

EnvironmentClass = Literal[
    "mw1.small",
    "mw1.medium",
    "mw1.large",
]

WebserverAccessMode = Literal[
    "PRIVATE_ONLY",
    "PUBLIC_ONLY",
]

Schedulers = Literal[
    2,
    3,
    4,
    5,
]
