"""
Registry package для управления компонентами pipeline framework
"""

from pipeline_core.registry.component_registry import (
    ComponentRegistry,
    ComponentInfo,
    DiscoverySource,
)

__all__ = ["ComponentRegistry", "ComponentInfo", "DiscoverySource"]
