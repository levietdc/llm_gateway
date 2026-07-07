"""
docs/research/semantic_cache.py

A prototype showing how to build a semantic cache for an AI gateway using RedisVL
(Redis Vector Library) and FastEmbed (offline embedding generation).

Dependencies required:
    pip install redisvl fastembed redis
"""

import sys
import os
from typing import Optional, Dict, Any, List

# FastEmbed imports
from fastembed import TextEmbedding

# RedisVL imports
from redisvl.index import SearchIndex
from redisvl.query import VectorQuery
from redis import Redis
from redis.exceptions import ConnectionError

class FastEmbedSemanticCache:
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        index_name: str = "llm_semantic_cache",
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        distance_metric: str = "cosine"
    ):
        """
        Initializes the semantic cache with FastEmbed and RedisVL.
        
        Args:
            redis_url: Connection string for Redis.
            index_name: Name of the Redis search index.
            model_name: FastEmbed model name (defaults to all-MiniLM-L6-v2, 384 dimensions).
            distance_metric: Distance metric for vector search ('cosine', 'ip', or 'l2').
        """
        print(f"Loading embedding model: {model_name}...")
        self.embedding_model = TextEmbedding(model_name=model_name)
        
        # Dimensions of sentence-transformers/all-MiniLM-L6-v2 is 384
        self.vector_dims = 384
        if "all-MiniLM-L6-v2" in model_name:
            self.vector_dims = 384
        
        self.redis_url = redis_url
        self.index_name = index_name
        self.distance_metric = distance_metric.lower()
        
        # Define the schema for RedisVL
        self.schema = {
            "index": {
                "name": self.index_name,
                "prefix": f"cache:{self.index_name}",
            },
            "fields": [
                {"name": "prompt", "type": "text"},
                {"name": "response", "type": "text"},
                {
                    "name": "prompt_vector",
                    "type": "vector",
                    "attrs": {
                        "dims": self.vector_dims,
                        "distance_metric": self.distance_metric,
                        "algorithm": "flat",  # Flat is excellent for smaller caches; HNSW is better for >1M records
                        "datatype": "float32"
                    }
                }
            ]
        }
        
        # Create SearchIndex instance
        self.index = SearchIndex.from_dict(self.schema)
        
    def connect_and_initialize(self, overwrite: bool = False):
        """
        Connect to Redis and create the index if it doesn't exist.
        """
        try:
            self.index.connect(self.redis_url)
            # Check if index already exists
            exists = self.index.exists()
            if not exists or overwrite:
                print(f"Creating search index '{self.index_name}' in Redis...")
                self.index.create(overwrite=overwrite)
            else:
                print(f"Index '{self.index_name}' already exists in Redis.")
        except ConnectionError as e:
            print(f"Failed to connect to Redis at {self.redis_url}. Ensure Redis is running.", file=sys.stderr)
            raise e

    def _get_embedding(self, text: str) -> List[float]:
        """Generates embedding vector for the text using FastEmbed."""
        embeddings = list(self.embedding_model.embed([text]))
        return embeddings[0].tolist()

    def set(self, prompt: str, response: str) -> str:
        """
        Cache a prompt and its response.
        
        Args:
            prompt: User prompt.
            response: LLM response.
            
        Returns:
            The generated key/ID in Redis.
        """
        vector = self._get_embedding(prompt)
        
        doc = {
            "prompt": prompt,
            "response": response,
            "prompt_vector": vector
        }
        
        # Load returns list of keys created
        keys = self.index.load([doc])
        print(f"Cached prompt. Key: {keys[0]}")
        return keys[0]

    def query(self, prompt: str, threshold: float = 0.90) -> Optional[Dict[str, Any]]:
        """
        Query the cache for a semantically similar prompt.
        
        Args:
            prompt: User prompt to query.
            threshold: Semantic similarity threshold [0.0, 1.0].
                       Under Cosine distance:
                       similarity = 1 - distance
                       Therefore, distance_threshold = 1 - threshold.
                       We filter for distance <= distance_threshold.
                       
        Returns:
            Dictionary with prompt and response if found and similarity >= threshold, else None.
        """
        query_vector = self._get_embedding(prompt)
        
        # Set up a Vector Query for the single most similar item
        vector_query = VectorQuery(
            vector=query_vector,
            vector_field_name="prompt_vector",
            return_fields=["prompt", "response"],
            num_results=1
        )
        
        results = self.index.query(vector_query)
        if not results:
            return None
            
        best_match = results[0]
        distance = float(best_match.get("vector_distance", 2.0))
        
        # Calculate similarity based on distance metric
        if self.distance_metric == "cosine":
            similarity = 1.0 - distance
        elif self.distance_metric == "ip":
            similarity = distance
        else:
            similarity = 1.0 / (1.0 + distance)
            
        print(f"Query: '{prompt}' -> Best Match: '{best_match.get('prompt')}' (Similarity: {similarity:.4f})")
        
        if similarity >= threshold:
            return {
                "prompt": best_match.get("prompt"),
                "response": best_match.get("response"),
                "similarity": similarity,
                "distance": distance
            }
        
        return None

# Self-contained testing execution
if __name__ == "__main__":
    cache = FastEmbedSemanticCache()
    
    try:
        # Connect and recreate the index for a fresh run
        cache.connect_and_initialize(overwrite=True)
    except ConnectionError:
        print("\n[SKIP] Skipping demo as Redis is not reachable. You can run Redis locally via:")
        print("    docker run -d -p 6379:6379 -p 8001:8001 redis/redis-stack:latest")
        sys.exit(0)
        
    print("\n--- Populating Semantic Cache ---")
    cache.set("What is the capital city of France?", "The capital of France is Paris.")
    cache.set("How far is the Moon from the Earth?", "The average distance from the Earth to the Moon is about 384,400 km (238,855 miles).")
    cache.set("What is the speed of light?", "The speed of light in a vacuum is exactly 299,792,458 meters per second.")

    print("\n--- Querying Semantic Cache ---")
    
    # Test case 1: Exact match
    print("\nTest Case 1: Exact match query")
    res1 = cache.query("What is the capital city of France?", threshold=0.90)
    print(f"Result: {res1['response'] if res1 else 'Cache Miss'}")
    
    # Test case 2: Semantic match (different phrasing)
    print("\nTest Case 2: Semantic match query (different phrasing)")
    res2 = cache.query("Can you tell me the capital of France?", threshold=0.85)
    print(f"Result: {res2['response'] if res2 else 'Cache Miss'}")
    
    # Test case 3: Distance query (different context)
    print("\nTest Case 3: Semantic match query (different phrasing)")
    res3 = cache.query("what's the distance between earth and the moon in miles?", threshold=0.80)
    print(f"Result: {res3['response'] if res3 else 'Cache Miss'}")

    # Test case 4: Cache miss (different topic)
    print("\nTest Case 4: Cache miss query")
    res4 = cache.query("Who wrote the play Romeo and Juliet?", threshold=0.80)
    print(f"Result: {res4['response'] if res4 else 'Cache Miss'}")
