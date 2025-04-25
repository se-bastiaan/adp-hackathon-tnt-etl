# Setup

## Database

Make sure you have a Snowflake account and a database. Create a schema in the database that you will use for this project.
Download the following dataset from Kaggle: [https://www.kaggle.com/datasets/jgassdfe/pokemon-dataset-of-gen-1-gen-9/data](https://www.kaggle.com/datasets/jgassdfe/pokemon-dataset-of-gen-1-gen-9/data).

Load the dataset into a `POKEMON_RAW` table in your Snowflake database. You can do this using the Snowflake web interface or any other tool you prefer.

Now create three tables in your Snowflake database from the `POKEMON_RAW` table. You can do this using the following SQL commands:
```sql
CREATE TABLE RAW_POKEMON_BASE AS SELECT ID, NAME, HEIGHT_INCHES, HEIGHT_METERS, WEIGHT_POUNDS, WEIGHT_KILOGRAMS, CLASSIFICATION_INFO, FORMS, GEN, IS_LEGENDARY, IS_MYTHICAL FROM RAW_POKEMON

CREATE or replace TABLE RAW_POKEMON_BATTLE AS SELECT ID, "Type 1", "Type 2", ABILITIES, HP, ATTACK, DEFENSE, "Sp. Attack", "Sp. Defense", "SPEED", "NORMAL_WEAKNESS", "FIRE_WEAKNESS", "WATER_WEAKNESS", "ELECTRIC_WEAKNESS", "GRASS_WEAKNESS", "ICE_WEAKNESS", FIGHTING_WEAKNESS, POISON_WEAKNESS,"GROUND_WEAKNESS",FLYING_WEAKNESS,PSYCHIC_WEAKNESS,BUG_WEAKNESS,ROCK_WEAKNESS,GHOST_WEAKNESS,DRAGON_WEAKNESS,DARK_WEAKNESS	STEEL_WEAKNESS,FAIRY_WEAKNESS FROM RAW_POKEMON

CREATE TABLE RAW_POKEMON_REPRODUCTION AS SELECT ID, CAPTURING_RATE, GENDER_MALE_RATIO, EGG_STEPS, EGG_CYCLES FROM RAW_POKEMON
```


## UV (recommended)

Install all dependencies using [uv](https://docs.astral.sh/uv/):

```
uv sync
```

## Python & Pip

Create a virtual environment and activate it.
```bash
python -m venv venv # or python3 -m venv venv depending on your system
source venv/bin/activate
# or on Windows:
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope Process
venv\Scripts\activate

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
