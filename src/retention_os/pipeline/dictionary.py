# src/retention_os/pipeline/dictionary.py
import logging
import yaml
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class FieldDefinition:
    """Definition of a field in the data dictionary."""
    
    def __init__(self, name: str, entity_type: str, data_type: str, 
                 description: str = "", example_values: List[Any] = None,
                 validation_rules: List[Dict] = None):
        """
        Initialize a field definition.
        
        Args:
            name: Field name
            entity_type: Entity this field belongs to
            data_type: Data type of the field
            description: Business description
            example_values: List of example values
            validation_rules: List of validation rules
        """
        self.name = name
        self.entity_type = entity_type
        self.data_type = data_type
        self.description = description
        self.example_values = example_values or []
        self.validation_rules = validation_rules or []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'name': self.name,
            'entity_type': self.entity_type,
            'data_type': self.data_type,
            'description': self.description,
            'example_values': self.example_values,
            'validation_rules': self.validation_rules
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FieldDefinition':
        """Create from dictionary representation."""
        return cls(
            name=data.get('name', ''),
            entity_type=data.get('entity_type', ''),
            data_type=data.get('data_type', ''),
            description=data.get('description', ''),
            example_values=data.get('example_values', []),
            validation_rules=data.get('validation_rules', [])
        )

class EntityDefinition:
    """Definition of an entity in the data dictionary."""
    
    def __init__(self, name: str, description: str = "", 
                 fields: Dict[str, FieldDefinition] = None,
                 relationships: List[Dict[str, str]] = None):
        """
        Initialize an entity definition.
        
        Args:
            name: Entity name
            description: Business description
            fields: Dictionary of field definitions
            relationships: List of relationships to other entities
        """
        self.name = name
        self.description = description
        self.fields = fields or {}
        self.relationships = relationships or []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'name': self.name,
            'description': self.description,
            'fields': {name: field.to_dict() for name, field in self.fields.items()},
            'relationships': self.relationships
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EntityDefinition':
        """Create from dictionary representation."""
        fields_dict = {}
        for name, field_data in data.get('fields', {}).items():
            fields_dict[name] = FieldDefinition.from_dict(field_data)
        
        return cls(
            name=data.get('name', ''),
            description=data.get('description', ''),
            fields=fields_dict,
            relationships=data.get('relationships', [])
        )

class DataDictionary:
    """Maintain metadata repository for all entities and fields."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the data dictionary.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.dictionary_path = self.config.get('path', 'data/dictionary/master_dictionary.yaml')
        self.auto_update = self.config.get('auto_update', True)
        self.entities = {}
        self.load_dictionary()
    
    def load_dictionary(self) -> None:
        """Load the data dictionary from file."""
        try:
            if os.path.exists(self.dictionary_path):
                with open(self.dictionary_path, 'r') as f:
                    data = yaml.safe_load(f)
                
                for entity_name, entity_data in data.get('entities', {}).items():
                    self.entities[entity_name] = EntityDefinition.from_dict(entity_data)
                
                logger.info(f"Loaded data dictionary with {len(self.entities)} entities")
            else:
                logger.warning(f"Data dictionary file not found: {self.dictionary_path}")
        except Exception as e:
            logger.error(f"Error loading data dictionary: {e}")
            # Create empty dictionary
            self.entities = {}
    
    def save_dictionary(self) -> None:
        """Save the data dictionary to file."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.dictionary_path), exist_ok=True)
            
            data = {
                'version': '1.0',
                'updated_at': datetime.now().isoformat(),
                'entities': {name: entity.to_dict() for name, entity in self.entities.items()}
            }
            
            with open(self.dictionary_path, 'w') as f:
                yaml.dump(data, f, default_flow_style=False)
            
            logger.info(f"Saved data dictionary to {self.dictionary_path}")
        except Exception as e:
            logger.error(f"Error saving data dictionary: {e}")
    
    def get_entity_definition(self, entity_type: str) -> Optional[EntityDefinition]:
        """
        Get the definition for an entity.
        
        Args:
            entity_type: Type of entity
            
        Returns:
            EntityDefinition or None if not found
        """
        return self.entities.get(entity_type)
    
    def get_field_definition(self, entity_type: str, field_name: str) -> Optional[FieldDefinition]:
        """
        Get the definition for a field.
        
        Args:
            entity_type: Type of entity
            field_name: Name of field
            
        Returns:
            FieldDefinition or None if not found
        """
        entity = self.get_entity_definition(entity_type)
        if entity:
            return entity.fields.get(field_name)
        return None
    
    def add_entity_definition(self, entity: EntityDefinition) -> None:
        """
        Add or update an entity definition.
        
        Args:
            entity: EntityDefinition to add
        """
        self.entities[entity.name] = entity
        if self.auto_update:
            self.save_dictionary()
    
    def add_field_definition(self, field: FieldDefinition) -> None:
        """
        Add or update a field definition.
        
        Args:
            field: FieldDefinition to add
        """
        entity_type = field.entity_type
        if entity_type not in self.entities:
            self.entities[entity_type] = EntityDefinition(name=entity_type)
        
        self.entities[entity_type].fields[field.name] = field
        if self.auto_update:
            self.save_dictionary()
    
    def register_validator(self, entity_type: str, field_name: str, rule: Dict[str, Any]) -> None:
        """
        Add a validation rule to a field.
        
        Args:
            entity_type: Type of entity
            field_name: Name of field
            rule: Validation rule definition
        """
        field = self.get_field_definition(entity_type, field_name)
        if field:
            field.validation_rules.append(rule)
            if self.auto_update:
                self.save_dictionary()
        else:
            logger.warning(f"Cannot add validation rule: field {entity_type}.{field_name} not found")
    
    def enrich_dictionary_from_profile(self, entity_type: str, profile: Dict[str, Any]) -> None:
        """
        Update dictionary based on profiling results.
        
        Args:
            entity_type: Type of entity
            profile: Data profile dictionary
        """
        if entity_type not in self.entities:
            self.entities[entity_type] = EntityDefinition(name=entity_type)
        
        entity = self.entities[entity_type]
        
        # Update entity with profile insights
        for column_profile in profile.get('columns', []):
            name = column_profile.get('name')
            if not name:
                continue
            
            # Get or create field definition
            if name not in entity.fields:
                data_type = column_profile.get('data_type', 'string')
                entity.fields[name] = FieldDefinition(name=name, entity_type=entity_type, data_type=data_type)
            
            field = entity.fields[name]
            
            # Update field based on profile
            field.data_type = column_profile.get('data_type', field.data_type)
            
            # Add example values
            if 'common_values' in column_profile:
                field.example_values = [item.get('value') for item in column_profile.get('common_values', [])[:5]]
            
            # Add validation rules based on profile
            if field.data_type.startswith(('int', 'float')) and 'min' in column_profile and 'max' in column_profile:
                min_value = column_profile.get('min')
                max_value = column_profile.get('max')
                
                # Add range validation rule
                range_rule = {
                    'rule_type': 'range',
                    'parameters': {
                        'min_value': min_value,
                        'max_value': max_value
                    }
                }
                
                # Check if rule already exists
                if not any(r.get('rule_type') == 'range' for r in field.validation_rules):
                    field.validation_rules.append(range_rule)
            
            # Add string pattern rules
            if field.data_type == 'string' and 'min_length' in column_profile and 'max_length' in column_profile:
                min_length = column_profile.get('min_length')
                max_length = column_profile.get('max_length')
                
                # Add length validation rule
                length_rule = {
                    'rule_type': 'length',
                    'parameters': {
                        'min_length': min_length,
                        'max_length': max_length
                    }
                }
                
                # Check if rule already exists
                if not any(r.get('rule_type') == 'length' for r in field.validation_rules):
                    field.validation_rules.append(length_rule)
        
        if self.auto_update:
            self.save_dictionary()
    
    def export_documentation(self, output_format: str = 'markdown', output_path: str = 'docs/data_dictionary') -> str:
        """
        Generate documentation for the data dictionary.
        
        Args:
            output_format: Format of the documentation ('markdown' or 'html')
            output_path: Path to write the documentation
            
        Returns:
            Path to the generated documentation
        """
        os.makedirs(output_path, exist_ok=True)
        
        if output_format == 'markdown':
            return self._export_markdown(output_path)
        elif output_format == 'html':
            return self._export_html(output_path)
        else:
            logger.warning(f"Unsupported documentation format: {output_format}")
            return ""
    
    def _export_markdown(self, output_path: str) -> str:
        """Generate Markdown documentation."""
        index_path = os.path.join(output_path, 'index.md')
        with open(index_path, 'w') as index_file:
            index_file.write("# Data Dictionary\n\n")
            index_file.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            index_file.write("## Entities\n\n")
            
            for entity_name in sorted(self.entities.keys()):
                index_file.write(f"- [{entity_name}]({entity_name}.md)\n")
        
        # Generate entity documentation
        for entity_name, entity in self.entities.items():
            entity_path = os.path.join(output_path, f"{entity_name}.md")
            with open(entity_path, 'w') as entity_file:
                entity_file.write(f"# {entity_name}\n\n")
                
                if entity.description:
                    entity_file.write(f"{entity.description}\n\n")
                
                entity_file.write("## Fields\n\n")
                entity_file.write("| Field | Type | Description | Validation |\n")
                entity_file.write("|-------|------|-------------|------------|\n")
                
                for field_name, field in sorted(entity.fields.items()):
                    validation = ", ".join([r.get('rule_type', '') for r in field.validation_rules])
                    entity_file.write(f"| {field_name} | {field.data_type} | {field.description} | {validation} |\n")
                
                if entity.relationships:
                    entity_file.write("\n## Relationships\n\n")
                    entity_file.write("| Related Entity | Relationship Type |\n")
                    entity_file.write("|---------------|------------------|\n")
                    
                    for rel in entity.relationships:
                        entity_file.write(f"| {rel.get('entity')} | {rel.get('type')} |\n")
        
        return index_path
    
    def _export_html(self, output_path: str) -> str:
        """Generate HTML documentation."""
        # Simple HTML export - could be enhanced
        index_path = os.path.join(output_path, 'index.html')
        with open(index_path, 'w') as index_file:
            index_file.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>Data Dictionary</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
    </style>
</head>
<body>
    <h1>Data Dictionary</h1>
    <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <h2>Entities</h2>
    <ul>
""")
            
            for entity_name in sorted(self.entities.keys()):
                index_file.write(f'        <li><a href="{entity_name}.html">{entity_name}</a></li>\n')
            
            index_file.write("""    </ul>
</body>
</html>""")
        
        # Generate entity documentation
        for entity_name, entity in self.entities.items():
            entity_path = os.path.join(output_path, f"{entity_name}.html")
            with open(entity_path, 'w') as entity_file:
                entity_file.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>Data Dictionary - {entity_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
    </style>
</head>
<body>
    <h1>{entity_name}</h1>
    <p><a href="index.html">Back to Index</a></p>
""")
                
                if entity.description:
                    entity_file.write(f"    <p>{entity.description}</p>\n")
                
                entity_file.write("""    <h2>Fields</h2>
    <table>
        <tr>
            <th>Field</th>
            <th>Type</th>
            <th>Description</th>
            <th>Validation</th>
        </tr>
""")
                
                for field_name, field in sorted(entity.fields.items()):
                    validation = ", ".join([r.get('rule_type', '') for r in field.validation_rules])
                    entity_file.write(f"""        <tr>
            <td>{field_name}</td>
            <td>{field.data_type}</td>
            <td>{field.description}</td>
            <td>{validation}</td>
        </tr>
""")
                
                entity_file.write("    </table>\n")
                
                if entity.relationships:
                    entity_file.write("""    <h2>Relationships</h2>
    <table>
        <tr>
            <th>Related Entity</th>
            <th>Relationship Type</th>
        </tr>
""")
                    
                    for rel in entity.relationships:
                        entity_file.write(f"""        <tr>
            <td>{rel.get('entity')}</td>
            <td>{rel.get('type')}</td>
        </tr>
""")
                    
                    entity_file.write("    </table>\n")
                
                entity_file.write("</body>\n</html>")
        
        return index_path