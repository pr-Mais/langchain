from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.documents import Document

from langchain_community.document_loaders.firestore import FirestoreLoader


@pytest.fixture
def raw_docs() -> List[Dict]:
    return [
        {"id": "1", "address": {"building": "1", "room": "1"}},
        {"id": "2", "address": {"building": "2", "room": "2"}},
    ]


@pytest.fixture
def expected_documents() -> List[Document]:
    return [
        Document(
            page_content="{'id': '1', 'address': {'building': '1', 'room': '1'}}",
            metadata={"database": "sample_restaurants", "collection": "restaurants"},
        ),
        Document(
            page_content="{'id': '2', 'address': {'building': '2', 'room': '2'}}",
            metadata={"database": "sample_restaurants", "collection": "restaurants"},
        ),
    ]


# @pytest.mark.requires("firebase-admin")
async def test_load_mocked(
    expected_documents: List[Document],
) -> None:
    # Mock the Firestore client's collection stream method
    mock_async_load = AsyncMock()
    mock_async_load.return_value = expected_documents

    # Mock Firestore client
    with patch(
        "firebase_admin.firestore.client",
        return_value=MagicMock(),
    ), patch(
        "langchain_community.document_loaders.firestore.FirestoreLoader.aload",
        new=mock_async_load,
    ):
        # Initialize FirestoreLoader
        loader = FirestoreLoader(
            project_id="test_project", collection_name="test_collection"
        )

        # Use the loader to load documents
        documents = await loader.aload()

    assert documents == expected_documents
