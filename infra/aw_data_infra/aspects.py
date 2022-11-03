"""
This module contains cdktf aspects.

Aspects walk the tree of resources in a stack and apply code to each.
"""

import jsii
from cdktf import (
    IAspect,
)
from constructs import IConstruct


@jsii.implements(IAspect)
class TagsAddingAspect:
    """Adds tags to AWS resource."""
    def __init__(self, tags_to_add: dict):
        self.tags_to_add = tags_to_add

    def visit(self, node: IConstruct):
        if hasattr(node, "tags"):
            if not isinstance(node.tags_input, dict):
                node.tags = {}
            node.tags = node.tags_input | self.tags_to_add
