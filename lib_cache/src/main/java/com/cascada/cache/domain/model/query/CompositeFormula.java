package com.cascada.cache.domain.model.query;

/** SQL-independent arithmetic plan compiled by the SQL adapter. */
public sealed interface CompositeFormula permits FormulaColumn, FormulaLiteral, FormulaBinary {}
