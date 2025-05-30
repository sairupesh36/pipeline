import unittest

from datatrove.pipeline.readers import HuggingFaceDatasetReader

from ..utils import require_datasets


@require_datasets
class TestHuggingFaceReader(unittest.TestCase):
    def test_read_dataset(self):
        reader = HuggingFaceDatasetReader(
            "truthful_qa", dataset_options={"name": "generation", "split": "validation"}, text_key="question"
        )
        data = list(reader())
        self.assertEqual(len(data), 817)

    def test_read_dataset_shuffle(self):
        reader = HuggingFaceDatasetReader(
            "truthful_qa",
            dataset_options={"name": "generation", "split": "validation"},
            text_key="question",
            shuffle_files=True,
        )
        data = list(reader())
        self.assertEqual(len(data[0].text), 69)
        self.assertEqual(len(data[1].text), 46)

    def test_read_streaming_dataset(self):
        reader = HuggingFaceDatasetReader(
            "truthful_qa",
            dataset_options={"name": "generation", "split": "validation"},
            text_key="question",
            streaming=True,
        )
        data = list(reader())
        self.assertEqual(len(data), 817)

    def test_read_streaming_dataset_shuffle(self):
        reader = HuggingFaceDatasetReader(
            "truthful_qa",
            dataset_options={"name": "generation", "split": "validation"},
            text_key="question",
            streaming=True,
            shuffle_files=True,
        )
        data = list(reader())
        self.assertEqual(len(data[0].text), 69)
        self.assertEqual(len(data[1].text), 46)

    def test_sharding(self):
        for shards in [1, 3]:
            for streaming in [True, False]:
                reader = HuggingFaceDatasetReader(
                    "huggingface/datatrove-tests",
                    dataset_options={"name": f"sharding-{shards}", "split": "train"},
                    text_key="text",
                    streaming=streaming,
                )
                # For streaming == True and sharding-3, the data is not contiguous
                # File1 -> ["hello", "world"], File2 -> ["how", "are"], File3 -> ["you"]
                # Because the data are taken non-contignous first shard gets File1 + File3
                # and second shard gets File2
                data0 = list(reader(rank=0, world_size=2))
                data1 = list(reader(rank=1, world_size=2))
                self.assertEqual(len(data0), 3)
                self.assertEqual(len(data1), 2)
