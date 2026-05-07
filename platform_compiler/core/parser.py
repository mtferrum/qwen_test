"""
SQL-DSL Parser

Parses SQL-DSL files and extracts:
- DEFINE TABLE statements
- DEFINE MODEL statements  
- DEFINE GRAPH statements
- CREATE VIEW statements
- INSERT/CREATE TABLE AS SELECT statements
"""

import re
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from ..models.schemas import (
    DSLLayer, TableDefinition, ModelDefinition, GraphDefinition,
    StreamConfig
)


class DSLParser:
    """Parser for SQL-DSL pipeline definitions."""

    # Regex patterns for DSL constructs
    DEFINE_TABLE_PATTERN = re.compile(
        r'DEFINE\s+TABLE\s+(\w+)\s*\((.*?)\)'
        r'(?:\s*WITH\s+STREAM\s*\((.*?)\))?',
        re.IGNORECASE | re.DOTALL
    )
    
    DEFINE_MODEL_PATTERN = re.compile(
        r'DEFINE\s+MODEL\s+(\w+)\s*\((.*?)\)',
        re.IGNORECASE | re.DOTALL
    )
    
    DEFINE_GRAPH_PATTERN = re.compile(
        r'DEFINE\s+GRAPH\s+(\w+)\s*\((.*?)\)',
        re.IGNORECASE | re.DOTALL
    )
    
    CREATE_VIEW_PATTERN = re.compile(
        r'CREATE\s+VIEW\s+(\w+)\s+AS\s+(.*?)(?=CREATE\s+VIEW|CREATE\s+TABLE|INSERT|$)',
        re.IGNORECASE | re.DOTALL
    )
    
    CREATE_TABLE_PATTERN = re.compile(
        r'CREATE\s+TABLE\s+(\w+)\s+AS\s+(.*?)(?=CREATE\s+VIEW|CREATE\s+TABLE|INSERT|$)',
        re.IGNORECASE | re.DOTALL
    )
    
    INSERT_PATTERN = re.compile(
        r'INSERT\s+INTO\s+(\w+)\s+SELECT\s+(.*?)(?=CREATE|INSERT|$)',
        re.IGNORECASE | re.DOTALL
    )

    def __init__(self):
        self.tables: List[TableDefinition] = []
        self.models: List[ModelDefinition] = []
        self.graphs: List[GraphDefinition] = []
        self.views: Dict[str, str] = {}
        self.inserts: List[str] = []

    def parse_file(self, file_path: str) -> DSLLayer:
        """Parse a DSL file from path."""
        content = Path(file_path).read_text()
        return self.parse_content(content)

    def parse_content(self, content: str) -> DSLLayer:
        """Parse DSL content string."""
        # Remove comments
        content = self._remove_comments(content)
        
        # Parse each construct type
        self._parse_tables(content)
        self._parse_models(content)
        self._parse_graphs(content)
        self._parse_views(content)
        self._parse_inserts(content)
        
        return DSLLayer(
            tables=self.tables,
            models=self.models,
            graphs=self.graphs,
            views=self.views,
            inserts=self.inserts
        )

    def _remove_comments(self, content: str) -> str:
        """Remove SQL comments (-- and /* */)."""
        # Remove single-line comments
        content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)
        # Remove multi-line comments
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
        return content

    def _parse_tables(self, content: str) -> None:
        """Parse DEFINE TABLE statements."""
        for match in self.DEFINE_TABLE_PATTERN.finditer(content):
            name = match.group(1)
            columns_str = match.group(2)
            stream_str = match.group(3)
            
            # Parse columns
            columns = self._parse_columns(columns_str)
            
            # Parse stream config if present
            stream_config = None
            if stream_str:
                stream_config = self._parse_stream_config(stream_str)
            
            self.tables.append(TableDefinition(
                name=name,
                columns=columns,
                stream_config=stream_config
            ))

    def _parse_columns(self, columns_str: str) -> Dict[str, str]:
        """Parse column definitions."""
        columns = {}
        # Split by comma, but handle nested types like ARRAY<FLOAT>
        parts = re.split(r',(?![^<]*>)', columns_str)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            # Match column name and type
            match = re.match(r'(\w+)\s+(.+)', part)
            if match:
                col_name = match.group(1)
                col_type = match.group(2).strip().upper()
                columns[col_name] = col_type
        return columns

    def _parse_stream_config(self, stream_str: str) -> StreamConfig:
        """Parse WITH STREAM configuration."""
        config = {}
        # Extract key=value pairs
        for match in re.finditer(r'(\w+)\s*=\s*(?:\'([^\']*)\'|(\w+))', stream_str):
            key = match.group(1)
            value = match.group(2) or match.group(3)
            config[key] = value
        
        return StreamConfig(
            time_attribute=config.get('time_attribute', ''),
            watermark=config.get('watermark'),
            allowed_lateness=config.get('allowed_lateness')
        )

    def _parse_models(self, content: str) -> None:
        """Parse DEFINE MODEL statements."""
        for match in self.DEFINE_MODEL_PATTERN.finditer(content):
            name = match.group(1)
            params_str = match.group(2)
            
            # Parse model parameters
            params = {}
            for m in re.finditer(r'(\w+)\s*=\s*(?:\'([^\']*)\'|(\w+)|(<[^>]+>))', params_str):
                key = m.group(1)
                value = m.group(2) or m.group(3) or m.group(4)
                params[key] = value
            
            self.models.append(ModelDefinition(
                name=name,
                path=params.get('path', ''),
                input_schema=params.get('input_schema', ''),
                output_schema=params.get('output_schema', '')
            ))

    def _parse_graphs(self, content: str) -> None:
        """Parse DEFINE GRAPH statements."""
        for match in self.DEFINE_GRAPH_PATTERN.finditer(content):
            name = match.group(1)
            body = match.group(2)
            
            # Extract vertices and edges
            vertices_match = re.search(r'vertices\s*=>\s*(\w+)\s*\(([^)]+)\)', body, re.IGNORECASE)
            edges_match = re.search(r'edges\s*=>\s*(\w+)\s*\(([^)]+)\)', body, re.IGNORECASE)
            
            if vertices_match and edges_match:
                v_table = vertices_match.group(1)
                v_cols = [c.strip() for c in vertices_match.group(2).split(',')]
                e_table = edges_match.group(1)
                e_cols = [c.strip() for c in edges_match.group(2).split(',')]
                
                self.graphs.append(GraphDefinition(
                    name=name,
                    vertices_table=v_table,
                    vertices_id_col=v_cols[0] if v_cols else '',
                    edges_table=e_table,
                    edges_src_col=e_cols[0] if e_cols else '',
                    edges_dst_col=e_cols[1] if len(e_cols) > 1 else ''
                ))

    def _parse_views(self, content: str) -> None:
        """Parse CREATE VIEW statements."""
        for match in self.CREATE_VIEW_PATTERN.finditer(content):
            name = match.group(1)
            query = match.group(2).strip()
            # Remove trailing semicolon if present
            query = query.rstrip(';').strip()
            self.views[name] = query

    def _parse_inserts(self, content: str) -> None:
        """Parse INSERT statements."""
        for match in self.INSERT_PATTERN.finditer(content):
            table = match.group(1)
            select_query = match.group(2).strip()
            statement = f"INSERT INTO {table} SELECT {select_query}"
            self.inserts.append(statement.rstrip(';'))
