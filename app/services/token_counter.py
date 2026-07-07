import logging
import anyio
import tiktoken
from tokenizers import Tokenizer

logger = logging.getLogger(__name__)

# Cache tokenizers to avoid reloading them on every call
_tokenizer_cache = {}

def get_openai_token_count(text: str, model: str = "gpt-4o") -> int:
    """
    Counts tokens for OpenAI models using tiktoken.
    """
    try:
        # Normalize model names to known tiktoken encodings if needed
        model_key = model
        if "gpt-4o" in model:
            model_key = "gpt-4o"
        elif "gpt-3.5" in model:
            model_key = "gpt-3.5-turbo"
        elif "gpt-4" in model:
            model_key = "gpt-4"
            
        if model_key not in _tokenizer_cache:
            _tokenizer_cache[model_key] = tiktoken.encoding_for_model(model_key)
        encoding = _tokenizer_cache[model_key]
        return len(encoding.encode(text))
    except Exception as e:
        logger.warning(f"Error getting tiktoken encoding for model {model}: {e}. Falling back to cl100k_base.")
        if "cl100k" not in _tokenizer_cache:
            _tokenizer_cache["cl100k"] = tiktoken.get_encoding("cl100k_base")
        return len(_tokenizer_cache["cl100k"].encode(text))

def get_anthropic_token_count(text: str, model: str = "claude-3-5-sonnet-20240620") -> int:
    """
    Counts tokens for Anthropic models using tokenizers or tiktoken fallback.
    """
    try:
        if "anthropic" not in _tokenizer_cache:
            try:
                # Attempt to load Xenova/claude-tokenizer from Hugging Face offline/online
                # If network is unavailable or fails, fallback to tiktoken's cl100k_base
                _tokenizer_cache["anthropic"] = Tokenizer.from_pretrained("Xenova/claude-tokenizer")
                logger.info("Successfully loaded Xenova/claude-tokenizer for Anthropic token counting.")
            except Exception as e:
                logger.warning(
                    f"Could not load Xenova/claude-tokenizer: {e}. "
                    "Falling back to tiktoken cl100k_base for Anthropic token counting."
                )
                _tokenizer_cache["anthropic"] = tiktoken.get_encoding("cl100k_base")
                
        tokenizer = _tokenizer_cache["anthropic"]
        if isinstance(tokenizer, Tokenizer):
            return len(tokenizer.encode(text).ids)
        else:
            # tiktoken encoding fallback
            return len(tokenizer.encode(text))
    except Exception as e:
        logger.error(f"Error in get_anthropic_token_count: {e}. Falling back to word estimation.")
        # Rough estimation: 1 word ~ 1.3 tokens
        return int(len(text.split()) * 1.3) + 1

def count_tokens(text: str, model: str) -> int:
    """
    Synchronous helper to count tokens based on model name.
    """
    if not text:
        return 0
        
    model_lower = model.lower()
    if "gpt-" in model_lower or "o1-" in model_lower or "gemini-" in model_lower:
        return get_openai_token_count(text, model)
    elif "claude-" in model_lower:
        return get_anthropic_token_count(text, model)
    else:
        # Fallback to general cl100k_base
        return get_openai_token_count(text, "gpt-4")

async def count_tokens_async(text: str, model: str) -> int:
    """
    Asynchronously count tokens. Since token counting is CPU-bound,
    runs the work in a separate worker thread to avoid blocking the event loop.
    """
    return await anyio.to_thread.run_sync(count_tokens, text, model)
