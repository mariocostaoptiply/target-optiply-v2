"""Optiply target class."""

from target_hotglue.target import TargetHotglue

from target_optiply.sinks import (
    BaseOptiplySink,
    ProductsSink,
    SupplierSink,
    SupplierProductSink,
    BuyOrderSink,
    BuyOrderLineSink,
    SellOrderSink,
    SellOrderLineSink,
    ProductCompositionSink,
)


class TargetOptiply(TargetHotglue):
    """Target for Optiply."""

    name = "target-optiply"
    MAX_PARALLELISM = 10
    SINK_TYPES = [
        BaseOptiplySink,
        ProductsSink,
        SupplierSink,
        SupplierProductSink,
        BuyOrderSink,
        BuyOrderLineSink,
        SellOrderSink,
        SellOrderLineSink,
        ProductCompositionSink,
    ]

    def get_sink_class(self, stream_name: str):
        return {
            "BuyOrders": BuyOrderSink,
            "Products": ProductsSink,
            "Suppliers": SupplierSink,
            "SupplierProducts": SupplierProductSink,
            "BuyOrderLines": BuyOrderLineSink,
            "SellOrders": SellOrderSink,
            "SellOrderLines": SellOrderLineSink,
            "ProductCompositions": ProductCompositionSink,
        }.get(stream_name, BaseOptiplySink)


if __name__ == "__main__":
    TargetOptiply.cli()
