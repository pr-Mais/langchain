import logging
from typing import List, Optional

from langchain_core.documents import Document

from langchain_community.document_loaders.base import BaseLoader

logger = logging.getLogger(__name__)

FIRESTORE_DEFAULT_DB = "(default)"


def _get_firestore_client(path: Optional[str] = None):
    """Get a Firestore client."""
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        raise ImportError(
            "Could not import firebase-admin python package. "
            "Please install it with `pip install firebase-admin`."
        )

    # Check if the app is already initialized
    try:
        firebase_admin.get_app()
    except ValueError as e:
        if path:
            logger.debug("Initializing Firebase app with credentials: %s", e)
            firebase_admin.initialize_app(credentials.Certificate(path))

        logger.debug("Initializing Firebase app with ADC: %s", e)
        firebase_admin.initialize_app()

    return firestore.client()


def _firestore_doc_converter(doc_snapshot) -> Document:
    """Convert a Firestore document to a Document object."""
    return Document(
        page_content=doc_snapshot.to_dict(),
        metadata={**doc_snapshot},
    )


class FirestoreLoader(BaseLoader):
    """Load Firestore documents."""

    def __init__(
        self,
        project_id: str,
        collection_name: str,
        credentials: Optional[str] = None,
        db_name: str = FIRESTORE_DEFAULT_DB,
        firestore_client=None,
    ) -> None:
        """
        Initialize a new instance of the FirestoreLoader class.

        :param project_id: The project ID for the Firestore instance.
        :param collection_name: The name of the collection to use.
        :param db_name: The name of the database to use,
        will use the `(default)` db if not provided.
        """

        if not collection_name:
            raise ValueError("collection_name must be provided.")

        self.project_id = project_id
        self.db_name = db_name

        # Initialize the Firestore client or use the provided one
        self.db = firestore_client or _get_firestore_client(credentials)

        # Create a reference to the collection
        self.collection = self.db.collection(collection_name)

    def load(self) -> List[Document]:
        """Load data into Document objects."""
        documents = []

        for doc in self.collection.stream():
            document = _firestore_doc_converter(doc)
            documents.append(document)

        return documents

    async def aload(self) -> List[Document]:
        """Load data into Document objects using the async method."""
        documents = []

        async for doc in self.collection.stream():
            document = _firestore_doc_converter(doc)
            documents.append(document)

        return documents
