"""
Парсер SQL-DSL.

Разбирает файлы SQL-DSL и извлекает:
- DEFINE TABLE - определения таблиц
- DEFINE MODEL - определения ML-моделей
- DEFINE GRAPH - определения графов
- CREATE VIEW - представления
- INSERT/CREATE TABLE AS SELECT - операции вставки
"""

import re
from pathlib import Path
from typing import Dict, List, Optional

from ..models.schemas import DSLLayer, TableDefinition, ModelDefinition, GraphDefinition, StreamConfig


class DSLParser:
    """
    Парсер для определений пайплайнов на SQL-DSL.
    
    Пример использования:
        parser = DSLParser()
        dsl = parser.parse_file('pipeline.dsl')
    """

    # Шаблоны регулярных выражений для конструкций DSL
    # Паттерн для DEFINE TABLE с поддержкой вложенных скобок и WITH STREAM
    DEFINE_TABLE_PATTERN = re.compile(
        r'DEFINE\s+TABLE\s+(\w+)\s*\((.*?)\)\s*(?:WITH\s+STREAM\s*\((.*?)\))?\s*;',
        re.IGNORECASE | re.DOTALL
    )
    
    DEFINE_MODEL_PATTERN = re.compile(
        r'DEFINE\s+MODEL\s+(\w+)\s*\((.*?)\)\s*;',
        re.IGNORECASE | re.DOTALL
    )
    
    DEFINE_GRAPH_PATTERN = re.compile(
        r'DEFINE\s+GRAPH\s+(\w+)\s*\((.*?)\)\s*;',
        re.IGNORECASE | re.DOTALL
    )
    
    CREATE_VIEW_PATTERN = re.compile(
        r'CREATE\s+VIEW\s+(\w+)\s+AS\s+(.*?)(?=CREATE\s+VIEW|CREATE\s+TABLE|INSERT|$)',
        re.IGNORECASE | re.DOTALL
    )
    
    CREATE_TABLE_AS_PATTERN = re.compile(
        r'CREATE\s+TABLE\s+(\w+)\s+AS\s+(.*?)(?=CREATE\s+VIEW|CREATE\s+TABLE|INSERT|$)',
        re.IGNORECASE | re.DOTALL
    )
    
    INSERT_PATTERN = re.compile(
        r'INSERT\s+INTO\s+(\w+)\s+SELECT\s+(.*?)(?=CREATE|INSERT|$)',
        re.IGNORECASE | re.DOTALL
    )

    def __init__(self):
        """Инициализация парсера."""
        self.tables: List[TableDefinition] = []
        self.models: List[ModelDefinition] = []
        self.graphs: List[GraphDefinition] = []
        self.views: Dict[str, str] = {}
        self.inserts: List[str] = []

    def parse_file(self, file_path: str) -> DSLLayer:
        """
        Разбор файла DSL.
        
        Args:
            file_path: Путь к файлу .dsl
            
        Returns:
            Объект DSLLayer с разобранными данными
        """
        content = Path(file_path).read_text()
        return self.parse_content(content)

    def parse_content(self, content: str) -> DSLLayer:
        """
        Разбор содержимого DSL из строки.
        
        Args:
            content: Строка с содержимым DSL
            
        Returns:
            Объект DSLLayer с разобранными данными
        """
        # Удаляем комментарии
        content = self._remove_comments(content)
        
        # Парсим каждую конструкцию
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
        """
        Удаление SQL-комментариев.
        
        Args:
            content: Исходное содержимое
            
        Returns:
            Содержимое без комментариев
        """
        # Однострочные комментарии (--)
        content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)
        # Многострочные комментарии (/* */)
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
        return content

    def _parse_tables(self, content: str) -> None:
        """
        Разбор определений таблиц (DEFINE TABLE).
        
        Args:
            content: Содержимое DSL
        """
        for match in self.DEFINE_TABLE_PATTERN.finditer(content):
            name = match.group(1)
            columns_str = match.group(2)
            stream_str = match.group(3)
            
            # Парсим колонки
            columns = self._parse_columns(columns_str)
            
            # Парсим конфигурацию стриминга если есть
            stream_config = None
            if stream_str:
                stream_config = self._parse_stream_config(stream_str)
            
            self.tables.append(TableDefinition(
                name=name,
                columns=columns,
                stream_config=stream_config
            ))

    def _parse_columns(self, columns_str: str) -> Dict[str, str]:
        """
        Разбор определений колонок.
        
        Args:
            columns_str: Строка с определениями колонок
            
        Returns:
            Словарь {имя_колонки: тип}
        """
        columns = {}
        # Разделяем по запятой, учитывая вложенные типы типа ARRAY<FLOAT>
        parts = re.split(r',(?![^<]*>)', columns_str)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            # Извлекаем имя и тип колонки
            match = re.match(r'(\w+)\s+(.+)', part)
            if match:
                col_name = match.group(1)
                col_type = match.group(2).strip().upper()
                columns[col_name] = col_type
        return columns

    def _parse_stream_config(self, stream_str: str) -> StreamConfig:
        """
        Разбор конфигурации WITH STREAM.
        
        Args:
            stream_str: Строка с параметрами стриминга
            
        Returns:
            Объект StreamConfig
        """
        config = {}
        # Извлекаем пары ключ=значение
        for m in re.finditer(r'(\w+)\s*=\s*(?:\'([^\']*)\'|(\w+))', stream_str):
            key = m.group(1)
            value = m.group(2) or m.group(3)
            config[key] = value
        
        return StreamConfig(
            time_attribute=config.get('time_attribute', ''),
            watermark=config.get('watermark'),
            allowed_lateness=config.get('allowed_lateness')
        )

    def _parse_models(self, content: str) -> None:
        """
        Разбор определений моделей (DEFINE MODEL).
        
        Args:
            content: Содержимое DSL
        """
        for match in self.DEFINE_MODEL_PATTERN.finditer(content):
            name = match.group(1)
            params_str = match.group(2)
            
            # Парсим параметры модели
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
        """
        Разбор определений графов (DEFINE GRAPH).
        
        Args:
            content: Содержимое DSL
        """
        for match in self.DEFINE_GRAPH_PATTERN.finditer(content):
            name = match.group(1)
            body = match.group(2)
            
            # Извлекаем вершины и рёбра
            v_match = re.search(r'vertices\s*=>\s*(\w+)\s*\(([^)]+)\)', body, re.IGNORECASE)
            e_match = re.search(r'edges\s*=>\s*(\w+)\s*\(([^)]+)\)', body, re.IGNORECASE)
            
            if v_match and e_match:
                v_table = v_match.group(1)
                v_cols = [c.strip() for c in v_match.group(2).split(',')]
                e_table = e_match.group(1)
                e_cols = [c.strip() for c in e_match.group(2).split(',')]
                
                self.graphs.append(GraphDefinition(
                    name=name,
                    vertices_table=v_table,
                    vertices_id_col=v_cols[0] if v_cols else '',
                    edges_table=e_table,
                    edges_src_col=e_cols[0] if e_cols else '',
                    edges_dst_col=e_cols[1] if len(e_cols) > 1 else ''
                ))

    def _parse_views(self, content: str) -> None:
        """
        Разбор представлений (CREATE VIEW).
        
        Args:
            content: Содержимое DSL
        """
        for match in self.CREATE_VIEW_PATTERN.finditer(content):
            name = match.group(1)
            query = match.group(2).strip()
            # Удаляем конечную точку с запятой
            query = query.rstrip(';').strip()
            self.views[name] = query
        
        # Также обрабатываем CREATE TABLE AS SELECT как view
        for match in self.CREATE_TABLE_AS_PATTERN.finditer(content):
            name = match.group(1)
            query = match.group(2).strip()
            query = query.rstrip(';').strip()
            self.views[name] = query

    def _parse_inserts(self, content: str) -> None:
        """
        Разбор INSERT операций.
        
        Args:
            content: Содержимое DSL
        """
        for match in self.INSERT_PATTERN.finditer(content):
            table = match.group(1)
            select_query = match.group(2).strip()
            statement = f"INSERT INTO {table} SELECT {select_query}"
            self.inserts.append(statement.rstrip(';'))
