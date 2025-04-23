# Setup

## UV (recommended)

Install all dependencies using [uv](https://docs.astral.sh/uv/):

```
uv sync
```

## Python & Pip

Create a virtual environment and activate it.
```bash
python -m venv venv # or python3 -m venv venv depending on your system
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

which python # Check that the virtual environment is activated on macOS/Linux
# or
where python # On Windows
```

Install the required packages.
```bash
pip install -r requirements.txt
```

# Usage

Copy the `.env.example` file to `.env` and fill in the required values.

```bash
cp .env.example .env # macOS/Linux
# or
copy .env.example .env # Windows
```

Start the server:

```python
uv run dagster dev
# or
python -m dagster dev
```
