# Example ETL pipeline
## Toolhub --> Toolhunt DB 

**This is a PoC and not production-ready/directly usable in our current setup, but can serve as insiration on how to refactor the db and streamline the populate_db job.**

The pipeline is based on the assumption that completed tasks are moved from Task into their own CompletedTask table upon completion. This makes it a lot easier to handle the cases of deleted, deprecated and completed tasks by ensuring they never make it into the Task table in the first place, and by deleting any tools that become deleted, deprecated or completed between two runs.

If a tool is deleted from the Tool table, all its associated tasks are automatically deleted from the Task table.
https://docs.sqlalchemy.org/en/20/orm/cascades.html#using-foreign-key-on-delete-cascade-with-orm-relationships

Tools that haven't received an update in the last X days are purged from the Tool table based on the timestamp in the last_updated column. In production, this would be somewhat risky as if for some reason the db_update_job hadn't run for n >= X days, all tools and incomplete tasks would be deleted from the db. Right now, the data loss wouldn't be catastrophic (just re-run the job to populate the tool and task atables again), but if we introduce more features/columns, e.g. to keep track of skips, it could become at least inconvenient.


### 0. Set up DB & models

This notebook assumes there's a running mysql or mariadb container mapped to port 3307 on the host, with a root user whose password is 'mypass', and that a db called 'mydb' has been created. It may also be necessary to set the  charset to utf8mb4.

`$ docker run --name mariadbtest -e MYSQL_ROOT_PASSWORD=mypass -p 3307:3306 -d docker.io/library/mariadb:10.4`

To get a mysql prompt in the terminal:

`$ mysql -P 3307 --protocol=TCP -u root -p`


```python
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import declarative_base, relationship, Session
```


```python
engine = create_engine('mysql+mysqldb://root:mypass@0.0.0.0:3307/mydb', echo=True)
```


```python
Base = declarative_base()

class Tool(Base):
    __tablename__ = 'tool'
    __table_args__ = {"mysql_charset": "utf8mb4"}

    name = Column(String(255), primary_key=True, nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(String(2047), nullable=False)
    url = Column(String(2047), nullable=False)
    tasks = relationship("Task", backref="tool", cascade="all, delete")
    last_updated = Column(DateTime, nullable=False)


class Task(Base):
    __tablename__ = "task"
    __table_args__ = {"mysql_charset": "utf8mb4"}

    id = Column(Integer, primary_key=True)
    tool_name = Column(String(255), ForeignKey("tool.name", ondelete="CASCADE"), nullable=False)
    field = Column(String(80), nullable=False)
    last_updated = Column(DateTime, nullable=False)
    last_attempted = Column(DateTime, nullable=True)


# Not part of the pipeline
class CompletedTask(Base):
    __tablename__ = "completed_task"
    __table_args__ = {"mysql_charset": "utf8mb4"}

    id = Column(Integer, primary_key=True)
    tool_name = Column(String(255), nullable=False)
    field = Column(String(80), nullable=False)
    user = Column(String(255), nullable=False)
    completed_date = Column(DateTime, nullable=False)

```


```python
Base.metadata.create_all(engine)
```

### 1. Extract


```python
import requests

from typing import Any, Iterator, Optional
```


```python
## Helpers

# Parameters
REQUEST_LABEL = 'ETL Tutorial'
USER_INFO = 'Phabricator user: Slst2020'
headers = {'User-Agent': f'{REQUEST_LABEL} - {USER_INFO}'}

TOOLS_API_ENDPOINT = "https://toolhub.wikimedia.org/api/tools"


# Functions
def fetch_all(url: str, **kwargs) -> list[dict[str, any]]:
    """Fetches all results from a paginated API endpoint."""
    results = []
    while url:
        response = requests.get(url, **kwargs)
        data = response.json()
        results.extend(data['results'])
        url = data['next']
    return results
```

### 2. Transform


```python
from dataclasses import dataclass
```


```python
## Helpers

# Parameters
ANNOTATIONS = {'audiences',
               'content_types',
               'tasks',
               'subject_domains',
               'wikidata_qid',
               'icon',
               'tool_type',
               'repository',
               'api_url',
               'translate_url',
               'bugtracker_url'}


# Functions
@dataclass
class ToolhuntTool:
    name: str
    title: str
    description: str
    url: str
    missing_annotations: set[str]
    deprecated: bool
    
    @property
    def is_completed(self) -> bool:
        return len(self.missing_annotations) == 0

    
def is_deprecated(tool: dict[str, Any]) -> bool:
    return tool["deprecated"] or tool["annotations"]["deprecated"]


def get_missing_annotations(tool_info: dict[str, Any], filter_by: set[str]=ANNOTATIONS) -> set[str]:
    missing = set()

    for k, v in tool_info['annotations'].items():
        value = v or tool_info.get(k, v)
        if value in (None, [], '') and k in filter_by:
            missing.add(k)

    return missing


def clean_tool_data(tool_data: list[dict[str, any]]) -> list[ToolhuntTool]:
    tools = []
    for tool in tool_data:
        t = ToolhuntTool(
                 name=tool["name"],
                 title=tool['title'],
                 description=tool['description'],
                 url=tool['url'],
                 missing_annotations=get_missing_annotations(tool),
                 deprecated=is_deprecated(tool)
                )
        if not t.deprecated and not t.is_completed:
            tools.append(t)
    return tools


```

### 3. Load 


```python
from sqlalchemy import delete
from sqlalchemy.dialects.mysql import insert
```


```python
## Helpers

def upsert_tool(tool: ToolhuntTool, engine, timestamp) -> None:
    """Inserts a tool in the Tool table if it doesn't exist, and updates it if it does."""
    
    insert_stmt = insert(Tool).values(
        name=tool.name,
        title=tool.title,
        description=tool.description,
        url=tool.url,
        last_updated=timestamp
    )
    on_duplicate_insert_stmt = insert_stmt.on_duplicate_key_update(
        title=tool.title, description=tool.description, last_updated=timestamp
    )
    session = Session(engine)
    session.execute(on_duplicate_insert_stmt)
    session.commit()

def remove_stale_tools(engine, expiration_days: int=7) -> None:
    """Removes expired tools from the Tool table."""
    limit = datetime.datetime.now() - datetime.timedelta(days=expiration_days)
    delete_stmt = delete(Tool).where(Tool.last_updated < limit)
    session = Session(engine)
    session.execute(delete_stmt)
    session.commit()


def update_tool_table(tools: list[ToolhuntTool], engine, timestamp, **kwargs) -> None:
    """Upserts tool records and removes stale tools"""
    
    [upsert_tool(tool, engine, timestamp) for tool in tools]
    
    remove_stale_tools(engine)
    

def upsert_task(tool_name: str, field: str, engine, timestamp) -> None:
    """Inserts a tool in the Tool table if it doesn't exist, and updates it if it does."""
    
    insert_stmt = insert(Task).values(
        tool_name=tool_name,
        field=field,
        last_updated=timestamp
    )
    on_duplicate_insert_stmt = insert_stmt.on_duplicate_key_update(last_updated=timestamp)

    session = Session(engine)
    session.execute(on_duplicate_insert_stmt)
    session.commit()


def update_task_table(tools: list[ToolhuntTool], engine, timestamp) -> None:
    """Upserts task records"""
    
    for tool in tools:
        tool_name = tool.name
        for field in tool.missing_annotations:
            upsert_task(tool_name, field, engine, timestamp)
    
```

###  4. Full Pipeline


```python
import datetime
```


```python
## Helpers

def run_pipeline(engine, **kwargs) -> None:
    # Extract
    tools_raw_data = fetch_all(TOOLS_API_ENDPOINT, headers=headers, params={'page_size': 100})
    # Transform
    tools_clean_data = clean_tool_data(tools_raw_data)
    # Load
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    update_tool_table(tools_clean_data, engine, timestamp)
    update_task_table(tools_clean_data, engine, timestamp)

```


```python
# This will populate the db if empty, or update all tool and task records if not.
run_pipeline(engine)
```
