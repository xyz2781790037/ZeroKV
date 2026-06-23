import unittest

from zerokv_vllm.key import Layout, Scope, build_complete_keys


class CacheKeyTest(unittest.TestCase):
    def test_go_cpp_golden_vector(self) -> None:
        scope = Scope(
            namespace="tenant-a",
            model_id="qwen2.5-7b",
            model_revision="abc123",
            adapter_id="lora-a",
            chunk_size=4,
            layout=Layout(
                version=1,
                dtype="fp32",
                layers=2,
                heads=4,
                head_dim=8,
            ),
        )
        keys = build_complete_keys(scope, [101, 2023, 2003, 1037, 3231, 102])
        self.assertEqual(len(keys), 1)
        self.assertEqual(
            keys[0].scope_digest.hex(),
            "965a666ddfbc9f1440089bced64913d9ed8d8da784028617ea75042c34b94623",
        )
        self.assertEqual(
            keys[0].prefix_digest.hex(),
            "656e4d82346551ff6379c043819b1f3a947abea0f85fb64f8b14c42762675731",
        )
        self.assertEqual(
            keys[0].object_id.hex(),
            "94e8969feafa7b823322771145804d1e37df2a73ac9a47184357aa40c92eb7b7",
        )
        self.assertEqual(keys[0].block_id, 9402384532672800916)

    def test_partial_tail_is_not_cacheable(self) -> None:
        scope = Scope(
            namespace="default",
            model_id="model",
            model_revision="revision",
            adapter_id="",
            chunk_size=4,
            layout=Layout(
                version=1,
                dtype="fp16",
                layers=1,
                heads=1,
                head_dim=1,
            ),
        )
        keys = build_complete_keys(scope, [1, 2, 3, 4, 5])
        self.assertEqual([(key.token_begin, key.token_end) for key in keys], [(0, 4)])


if __name__ == "__main__":
    unittest.main()
