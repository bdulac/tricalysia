package io.github.bdulac.tricalysia.gremlin;

import java.util.UUID;

import org.neo4j.driver.v1.types.Entity;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;

public class Neo4JCustomIdProvider implements Neo4JElementIdProvider<Integer> {

	@Override
	public Integer generate() {
		return UUID.randomUUID().hashCode();
	}

	@Override
	public String fieldName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer get(Entity entity) {
		return entity.hashCode();
	}

	@Override
	public Integer processIdentifier(Object id) {
		if(id instanceof Integer) {
			return (Integer)id;
		}
		return null;
	}

	@Override
	public String matchPredicateOperand(String alias) {
		if(alias == null)return null;
		 return "ID(" + alias + ")";
	}

}
