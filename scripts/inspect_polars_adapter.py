from pathlib import Path

from app.core.processing_adapters import build_polars_order_plan


INPUT_PATH = Path("tests/golden/day5/accepted.ndjson")


def main() -> None:
    lazy_frame = build_polars_order_plan(INPUT_PATH)

    print("Schema:")
    print(lazy_frame.collect_schema())

    print("\nLogical plan:")
    print(lazy_frame.explain())

    print("\nStreaming physical plan:")
    print(
        lazy_frame.show_graph(
            show=False,
            raw_output=True,
            engine="streaming",
            plan_stage="physical",
        )
    )

    print("\nStreaming result:")
    result = lazy_frame.collect(engine="streaming")
    print(result)


if __name__ == "__main__":
    main()
