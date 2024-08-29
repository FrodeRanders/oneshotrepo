/*
 * Copyright (C) 2024 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.repo.search;

import org.gautelis.repo.exceptions.InvalidParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;


/**
 *
 */
public class SearchExpression {
    private static final Logger log = LoggerFactory.getLogger(SearchExpression.class);
    //
    private SearchExpression expr1 = null;
    private SearchExpression expr2 = null;
    private SearchItem<?> item = null;
    private Operator op = Operator.NOP;

    public SearchExpression(
            SearchExpression expr1, SearchExpression expr2, Operator op
    ) throws InvalidParameterException {
        Objects.requireNonNull(expr1, "expr1");
        Objects.requireNonNull(expr2, "expr2");

        this.expr1 = expr1;
        this.expr2 = expr2;
        this.op = op;
    }

    public SearchExpression(
            SearchItem<?> item, boolean notSearch
    ) throws InvalidParameterException {
        Objects.requireNonNull(item, "item");
        if (notSearch) {
            this.op = Operator.NOT;
        }
        this.item = item;
    }

    public SearchExpression(
            SearchItem<?> item
    ) throws InvalidParameterException {
        this(item, /* not? */ false);
    }

    public SearchExpression(
            SearchExpression expr, boolean notSearch
    ) throws InvalidParameterException {
        Objects.requireNonNull(expr, "expr");
        if (notSearch) {
            this.op = Operator.NOT;
        }
        this.expr1 = expr;
    }

    public SearchExpression(
            SearchExpression expr
    ) throws InvalidParameterException {
        this(expr, /* not? */ false);
    }

    /**
     * Helper method that adds a search item to an expression with logical
     * operator AND (item).
     *
     * @param expr existing expression
     * @param item item to add
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleAnd(
            SearchExpression expr, SearchItem<?> item
    ) throws InvalidParameterException {

        SearchExpression e = new SearchExpression(item, /* not? */ false);
        return new SearchExpression(expr, e, Operator.AND);
    }

    /**
     * Helper method that adds a search item to an expression with logical
     * operator AND NOT (item).
     *
     * @param expr existing expression
     * @param item item to add
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleAndNot(
            SearchExpression expr, SearchItem<?> item
    ) throws InvalidParameterException {

        SearchExpression e = new SearchExpression(item, /* not? */ true);
        return new SearchExpression(expr, e, Operator.AND);
    }

    /**
     * Helper method that adds a search item to an expression with logical
     * operator OR (item).
     *
     * @param expr existing expression
     * @param item item to add
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleOr(
            SearchExpression expr, SearchItem<?> item
    ) throws InvalidParameterException {

        SearchExpression e = new SearchExpression(item, /* not? */ false);
        return new SearchExpression(expr, e, Operator.OR);
    }

    /**
     * Helper method that adds a search item to an expression with logical
     * operator OR NOT (item).
     *
     * @param expr existing expression
     * @param item item to add
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleOrNot(
            SearchExpression expr, SearchItem<?> item
    ) throws InvalidParameterException {

        SearchExpression e = new SearchExpression(item, /* not? */ true);
        return new SearchExpression(expr, e, Operator.OR);
    }

    public SearchExpression getExpression1() {
        return expr1;
    }

    public SearchExpression getExpression2() {
        return expr2;
    }

    public SearchItem<?> getItem() {
        return item;
    }

    public Operator getOperator() {
        return op;
    }
}
