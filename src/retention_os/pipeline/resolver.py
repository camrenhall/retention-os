import logging
from typing import Dict, List, Any, Optional, Set, Tuple
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz

logger = logging.getLogger(__name__)

class MatchRule:
    """Rule for matching entities."""
    
    def __init__(self, 
                 strategy: str, 
                 fields: List[str], 
                 threshold: float = 0.7, 
                 weight: float = 1.0):
        """
        Initialize a match rule.
        
        Args:
            strategy: The matching strategy (exact, fuzzy, etc.)
            fields: List of fields to use for matching
            threshold: Threshold for fuzzy matching
            weight: Weight of this rule in the overall score
        """
        self.strategy = strategy
        self.fields = fields
        self.threshold = threshold
        self.weight = weight
    
    def __str__(self) -> str:
        """String representation of the rule."""
        return f"{self.strategy} match on {', '.join(self.fields)} (threshold: {self.threshold}, weight: {self.weight})"

class EntityMatch:
    """Represents a match between two entities."""
    
    def __init__(self, 
                 entity1_id: str, 
                 entity2_id: str, 
                 confidence: float, 
                 match_type: str, 
                 matched_fields: Dict[str, Any]):
        """
        Initialize an entity match.
        
        Args:
            entity1_id: ID of the first entity
            entity2_id: ID of the second entity
            confidence: Match confidence score
            match_type: Type of match (exact, fuzzy, etc.)
            matched_fields: Fields that matched
        """
        self.entity1_id = entity1_id
        self.entity2_id = entity2_id
        self.confidence = confidence
        self.match_type = match_type
        self.matched_fields = matched_fields
    
    def __str__(self) -> str:
        """String representation of the match."""
        return f"Match({self.entity1_id}, {self.entity2_id}, confidence={self.confidence:.2f}, type={self.match_type})"

class EntityResolver:
    """Resolves entities across different source files."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the entity resolver.
        
        Args:
            config: Configuration dictionary for entity resolution
        """
        self.config = config or {}
        self.confidence_threshold = self.config.get('confidence_threshold', 0.7)
        self.strategies = self.config.get('strategies', ['exact_match', 'fuzzy_name_match'])
    
    def resolve_entities(self, 
                         entities: List[Dict[str, Any]], 
                         rules: List[MatchRule]) -> Tuple[List[Dict[str, Any]], List[EntityMatch]]:
        """
        Resolve entities using the specified rules.
        
        Args:
            entities: List of entity dictionaries
            rules: List of match rules
            
        Returns:
            Tuple of (resolved_entities, matches)
        """
        if len(entities) <= 1:
            return entities, []
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(entities)
        
        # Generate matches
        matches = self._find_matches(df, rules)
        
        # Build clusters of matched entities
        clusters = self._build_clusters(df['_id'].tolist(), matches)
        
        # Merge entities within each cluster
        resolved_entities = self._merge_entities(df, clusters)
        
        return resolved_entities, matches
    
    def _find_matches(self, df: pd.DataFrame, rules: List[MatchRule]) -> List[EntityMatch]:
        """
        Find matches between entities using the specified rules.
        
        Args:
            df: DataFrame of entities
            rules: List of match rules
            
        Returns:
            List of EntityMatch objects
        """
        matches = []
        
        # Add ID column if not present
        if '_id' not in df.columns:
            df['_id'] = [str(i) for i in range(len(df))]
        
        # Process each rule
        for rule in rules:
            # Skip rule if strategy not enabled
            if rule.strategy not in self.strategies:
                continue
            
            # Check if all required fields are present
            if not all(field in df.columns for field in rule.fields):
                logger.warning(f"Skipping rule {rule}: missing fields")
                continue
            
            # Find matches based on strategy
            if rule.strategy == 'exact_match':
                new_matches = self._exact_match(df, rule)
            elif rule.strategy == 'fuzzy_name_match':
                new_matches = self._fuzzy_name_match(df, rule)
            elif rule.strategy == 'composite_key_match':
                new_matches = self._composite_key_match(df, rule)
            else:
                logger.warning(f"Unsupported match strategy: {rule.strategy}")
                continue
            
            matches.extend(new_matches)
        
        # Filter out low confidence matches
        return [m for m in matches if m.confidence >= self.confidence_threshold]
    
    def _exact_match(self, df: pd.DataFrame, rule: MatchRule) -> List[EntityMatch]:
        """
        Perform exact matching on specified fields.
        
        Args:
            df: DataFrame of entities
            rule: Match rule
            
        Returns:
            List of EntityMatch objects
        """
        matches = []
        
        # Get the primary field for matching
        match_field = rule.fields[0]
        
        # Group by the match field
        groups = df.groupby(match_field)
        
        # Find groups with more than one entity
        for field_value, group in groups:
            if len(group) <= 1 or pd.isna(field_value) or field_value == "":
                continue
            
            # Generate all pairs within the group
            entities = group.to_dict('records')
            for i, entity1 in enumerate(entities):
                for entity2 in entities[i+1:]:
                    match = EntityMatch(
                        entity1_id=entity1['_id'],
                        entity2_id=entity2['_id'],
                        confidence=1.0,  # Exact match has 100% confidence
                        match_type='exact',
                        matched_fields={match_field: field_value}
                    )
                    matches.append(match)
        
        return matches
    
    def _fuzzy_name_match(self, df: pd.DataFrame, rule: MatchRule) -> List[EntityMatch]:
        """
        Perform fuzzy matching on name fields.
        
        Args:
            df: DataFrame of entities
            rule: Match rule
            
        Returns:
            List of EntityMatch objects
        """
        matches = []
        
        # Get name fields
        name_fields = rule.fields
        
        # Generate all pairs of entities
        entities = df.to_dict('records')
        for i, entity1 in enumerate(entities):
            for entity2 in entities[i+1:]:
                matched_fields = {}
                total_score = 0
                
                # Compare each name field
                for field in name_fields:
                    val1 = str(entity1.get(field, "")).lower()
                    val2 = str(entity2.get(field, "")).lower()
                    
                    # Skip empty values
                    if not val1 or not val2:
                        continue
                    
                    # Calculate similarity
                    similarity = fuzz.ratio(val1, val2) / 100.0
                    
                    if similarity >= rule.threshold:
                        matched_fields[field] = (val1, val2, similarity)
                        total_score += similarity
                
                # Calculate average score
                if matched_fields:
                    avg_score = total_score / len(matched_fields)
                    
                    # Create match if score is high enough
                    if avg_score >= rule.threshold:
                        match = EntityMatch(
                            entity1_id=entity1['_id'],
                            entity2_id=entity2['_id'],
                            confidence=avg_score,
                            match_type='fuzzy',
                            matched_fields=matched_fields
                        )
                        matches.append(match)
        
        return matches
    
    def _composite_key_match(self, df: pd.DataFrame, rule: MatchRule) -> List[EntityMatch]:
        """
        Perform matching on a composite key (multiple fields).
        
        Args:
            df: DataFrame of entities
            rule: Match rule
            
        Returns:
            List of EntityMatch objects
        """
        matches = []
        
        # Create composite key
        df['_composite_key'] = df.apply(
            lambda row: tuple(str(row.get(field, "")).lower() for field in rule.fields),
            axis=1
        )
        
        # Group by composite key
        groups = df.groupby('_composite_key')
        
        # Find groups with more than one entity
        for key, group in groups:
            if len(group) <= 1 or any(pd.isna(k) or k == "" for k in key):
                continue
            
            # Generate all pairs within the group
            entities = group.to_dict('records')
            for i, entity1 in enumerate(entities):
                for entity2 in entities[i+1:]:
                    matched_fields = {
                        field: (str(entity1.get(field, "")), str(entity2.get(field, "")))
                        for field in rule.fields
                    }
                    
                    match = EntityMatch(
                        entity1_id=entity1['_id'],
                        entity2_id=entity2['_id'],
                        confidence=1.0,  # Composite key match has 100% confidence
                        match_type='composite',
                        matched_fields=matched_fields
                    )
                    matches.append(match)
        
        return matches
    
    def _build_clusters(self, entity_ids: List[str], matches: List[EntityMatch]) -> List[Set[str]]:
        """
        Build clusters of matched entities.
        
        Args:
            entity_ids: List of entity IDs
            matches: List of EntityMatch objects
            
        Returns:
            List of sets, where each set contains IDs of entities in the same cluster
        """
        # Initialize disjoint set
        parent = {id: id for id in entity_ids}
        
        # Find function for disjoint set
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        # Union function for disjoint set
        def union(x, y):
            parent[find(x)] = find(y)
        
        # Union matched pairs
        for match in matches:
            union(match.entity1_id, match.entity2_id)
        
        # Build clusters
        clusters = {}
        for id in entity_ids:
            root = find(id)
            if root not in clusters:
                clusters[root] = set()
            clusters[root].add(id)
        
        # Return clusters with more than one entity
        return [cluster for cluster in clusters.values() if len(cluster) > 1]
    
    def _merge_entities(self, df: pd.DataFrame, clusters: List[Set[str]]) -> List[Dict[str, Any]]:
        """
        Merge entities within each cluster.
        
        Args:
            df: DataFrame of entities
            clusters: List of clusters of entity IDs
            
        Returns:
            List of merged entity dictionaries
        """
        # Convert DataFrame to dict for easier manipulation
        entities_by_id = {row['_id']: row.to_dict() for _, row in df.iterrows()}
        
        # Start with unclustered entities
        all_clustered_ids = set()
        for cluster in clusters:
            all_clustered_ids.update(cluster)
        
        unclustered_ids = set(entities_by_id.keys()) - all_clustered_ids
        merged_entities = [entities_by_id[id] for id in unclustered_ids]
        
        # Merge each cluster
        for cluster in clusters:
            cluster_entities = [entities_by_id[id] for id in cluster]
            merged = self._merge_entity_group(cluster_entities)
            merged_entities.append(merged)
        
        return merged_entities
    
    def _merge_entity_group(self, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge a group of entities into a single entity.
        
        Args:
            entities: List of entity dictionaries to merge
            
        Returns:
            Merged entity dictionary
        """
        if not entities:
            return {}
        
        # Start with the first entity
        merged = entities[0].copy()
        
        # Track which fields came from which entity
        merged['_merged_from'] = [entities[0]['_id']]
        
        # Merge remaining entities
        for entity in entities[1:]:
            for key, value in entity.items():
                # Skip ID and internal fields
                if key == '_id' or key.startswith('_'):
                    continue
                
                # Handle the merge based on field type
                if key not in merged or pd.isna(merged[key]) or merged[key] == "":
                    # If field is missing or empty in merged, use the entity's value
                    merged[key] = value
                elif pd.isna(value) or value == "":
                    # If value is empty, keep the merged value
                    pass
                elif isinstance(value, list) and isinstance(merged[key], list):
                    # Merge lists
                    merged[key] = list(set(merged[key] + value))
                elif isinstance(value, dict) and isinstance(merged[key], dict):
                    # Merge dictionaries
                    merged[key].update(value)
                # Otherwise keep the original value
            
            # Track the merge
            merged['_merged_from'].append(entity['_id'])
        
        return merged