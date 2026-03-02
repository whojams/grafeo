//! GQL Parser.

use super::ast::*;
use super::lexer::{Lexer, Token, TokenKind};
use grafeo_common::utils::error::{Error, QueryError, QueryErrorKind, Result, SourceSpan};

/// Unescapes backslash-escaped characters in a string literal.
fn unescape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('\'') => result.push('\''),
                Some('"') => result.push('"'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(ch);
        }
    }
    result
}

/// GQL Parser.
pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current: Token,
    peeked: Option<Token>,
    source: &'a str,
}

impl<'a> Parser<'a> {
    /// Creates a new parser for the given input.
    pub fn new(input: &'a str) -> Self {
        let mut lexer = Lexer::new(input);
        let current = lexer.next_token();
        Self {
            lexer,
            current,
            peeked: None,
            source: input,
        }
    }

    /// Checks if the current token can be used as a label or type name.
    /// This includes identifiers, quoted identifiers, and certain reserved keywords that are
    /// commonly used as labels (Node, Edge, Type, etc.)
    fn is_label_or_type_name(&self) -> bool {
        matches!(
            self.current.kind,
            TokenKind::Identifier
                | TokenKind::QuotedIdentifier
                | TokenKind::Node
                | TokenKind::Edge
                | TokenKind::Type
                | TokenKind::Match
                | TokenKind::Return
                | TokenKind::Where
                | TokenKind::And
                | TokenKind::Or
                | TokenKind::Not
                | TokenKind::Insert
                | TokenKind::Delete
                | TokenKind::Set
                | TokenKind::Create
                | TokenKind::As
                | TokenKind::Distinct
                | TokenKind::Order
                | TokenKind::By
                | TokenKind::Asc
                | TokenKind::Desc
                | TokenKind::Limit
                | TokenKind::Skip
                | TokenKind::With
                | TokenKind::Optional
                | TokenKind::Null
                | TokenKind::True
                | TokenKind::False
                | TokenKind::In
                | TokenKind::Is
                | TokenKind::Like
                | TokenKind::Case
                | TokenKind::When
                | TokenKind::Then
                | TokenKind::Else
                | TokenKind::End
                | TokenKind::Exists
                | TokenKind::Call
                | TokenKind::Yield
                | TokenKind::Detach
                | TokenKind::Unwind
                | TokenKind::Merge
                | TokenKind::On
                | TokenKind::Starts
                | TokenKind::Ends
                | TokenKind::Contains
        )
    }

    /// Checks if the current token is an identifier (regular or backtick-quoted).
    fn is_identifier(&self) -> bool {
        matches!(
            self.current.kind,
            TokenKind::Identifier | TokenKind::QuotedIdentifier
        ) || self.is_contextual_keyword()
    }

    /// Checks if the current token is a keyword that can be used as an identifier in context.
    /// In GQL/Cypher, many keywords can be used as variable names or labels.
    fn is_contextual_keyword(&self) -> bool {
        matches!(
            self.current.kind,
            TokenKind::End       // CASE...END
                | TokenKind::Node    // CREATE NODE TYPE
                | TokenKind::Edge    // CREATE EDGE TYPE
                | TokenKind::Type    // type() function
                | TokenKind::Case    // CASE expression
                | TokenKind::When    // CASE WHEN
                | TokenKind::Then    // CASE THEN
                | TokenKind::Else    // CASE ELSE
                | TokenKind::In      // IN operator (can be label/variable)
                | TokenKind::Is      // IS NULL
                | TokenKind::And     // AND operator
                | TokenKind::Or      // OR operator
                | TokenKind::Not     // NOT operator
                | TokenKind::Null    // NULL literal
                | TokenKind::True    // TRUE literal
                | TokenKind::False   // FALSE literal
                | TokenKind::Vector  // vector() function
                | TokenKind::Index   // index-related usage
                | TokenKind::Dimension // dimension option
                | TokenKind::Metric // metric option
        )
    }

    /// Gets the identifier name from the current token.
    /// For quoted identifiers, strips the backticks.
    fn get_identifier_name(&self) -> String {
        let text = &self.current.text;
        if self.current.kind == TokenKind::QuotedIdentifier {
            // Strip backticks from `name` -> name
            text[1..text.len() - 1].to_string()
        } else {
            text.clone()
        }
    }

    /// Parses the input into a statement.
    pub fn parse(&mut self) -> Result<Statement> {
        let mut left = self.parse_single_statement()?;

        // Handle NEXT (linear composition): output of left becomes input of right
        while self.is_identifier() && self.get_identifier_name().eq_ignore_ascii_case("NEXT") {
            self.advance(); // consume NEXT
            let right = self.parse_single_statement()?;
            // NEXT semantics: chain right after left (like WITH pipe).
            // Represent as CompositeQuery with a dedicated op.
            left = Statement::CompositeQuery {
                left: Box::new(left),
                op: CompositeOp::Next,
                right: Box::new(right),
            };
        }

        // Check for composite query operators (UNION, EXCEPT, INTERSECT, OTHERWISE)
        while matches!(
            self.current.kind,
            TokenKind::Union | TokenKind::Except | TokenKind::Intersect | TokenKind::Otherwise
        ) {
            let op = match self.current.kind {
                TokenKind::Union => {
                    self.advance();
                    if self.current.kind == TokenKind::All {
                        self.advance();
                        CompositeOp::UnionAll
                    } else {
                        CompositeOp::Union
                    }
                }
                TokenKind::Except => {
                    self.advance();
                    CompositeOp::Except
                }
                TokenKind::Intersect => {
                    self.advance();
                    CompositeOp::Intersect
                }
                TokenKind::Otherwise => {
                    self.advance();
                    CompositeOp::Otherwise
                }
                _ => unreachable!(),
            };
            let right = self.parse_single_statement()?;
            left = Statement::CompositeQuery {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_single_statement(&mut self) -> Result<Statement> {
        match self.current.kind {
            TokenKind::Match
            | TokenKind::Optional
            | TokenKind::Unwind
            | TokenKind::Merge
            | TokenKind::For => self.parse_query().map(Statement::Query),
            TokenKind::Insert => self
                .parse_insert()
                .map(|s| Statement::DataModification(DataModificationStatement::Insert(s))),
            TokenKind::Delete => self
                .parse_delete()
                .map(|s| Statement::DataModification(DataModificationStatement::Delete(s))),
            TokenKind::Create => {
                // Check if CREATE is followed by a pattern (Cypher-style) or NODE/EDGE (GQL schema)
                let next = self.peek_kind();
                if next == TokenKind::LParen {
                    // Cypher-style: CREATE (n:Label {...}) - treat as INSERT
                    self.parse_create_as_insert()
                        .map(|s| Statement::DataModification(DataModificationStatement::Insert(s)))
                } else {
                    // GQL schema: CREATE NODE TYPE / CREATE EDGE TYPE
                    self.parse_create_schema().map(Statement::Schema)
                }
            }
            TokenKind::Call => self.parse_call_statement().map(Statement::Call),
            _ => {
                Err(self
                    .error("Expected MATCH, INSERT, DELETE, MERGE, UNWIND, FOR, CREATE, or CALL"))
            }
        }
    }

    /// Parses a CALL procedure statement.
    ///
    /// ```text
    /// CALL name.space(args) [YIELD field [AS alias], ...]
    /// ```
    fn parse_call_statement(&mut self) -> Result<CallStatement> {
        let span_start = self.current.span.start;
        self.expect(TokenKind::Call)?;

        // Parse dotted procedure name: ident { . ident }
        if !self.is_identifier() {
            return Err(self.error("Expected procedure name after CALL"));
        }
        let mut name_parts = vec![self.get_identifier_name()];
        self.advance();
        while self.current.kind == TokenKind::Dot {
            self.advance();
            if !self.is_identifier() {
                return Err(self.error("Expected identifier after '.'"));
            }
            name_parts.push(self.get_identifier_name());
            self.advance();
        }

        // Parse argument list: ( [expr { , expr }] )
        self.expect(TokenKind::LParen)?;
        let mut arguments = Vec::new();
        if self.current.kind != TokenKind::RParen {
            arguments.push(self.parse_expression()?);
            while self.current.kind == TokenKind::Comma {
                self.advance();
                arguments.push(self.parse_expression()?);
            }
        }
        self.expect(TokenKind::RParen)?;

        // Parse optional YIELD clause
        let yield_items = if self.current.kind == TokenKind::Yield {
            self.advance();
            Some(self.parse_yield_list()?)
        } else {
            None
        };

        // Parse optional WHERE clause (only valid after YIELD)
        let where_clause = if yield_items.is_some() && self.current.kind == TokenKind::Where {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        // Parse optional RETURN clause (only valid after YIELD)
        let return_clause = if yield_items.is_some() && self.current.kind == TokenKind::Return {
            Some(self.parse_return_clause()?)
        } else {
            None
        };

        Ok(CallStatement {
            procedure_name: name_parts,
            arguments,
            yield_items,
            where_clause,
            return_clause,
            span: Some(SourceSpan::new(span_start, self.current.span.start, 1, 1)),
        })
    }

    /// Parses a YIELD item list: `field [AS alias] { , field [AS alias] }`.
    fn parse_yield_list(&mut self) -> Result<Vec<YieldItem>> {
        let mut items = vec![self.parse_yield_item()?];
        while self.current.kind == TokenKind::Comma {
            self.advance();
            items.push(self.parse_yield_item()?);
        }
        Ok(items)
    }

    /// Parses a single YIELD item: `field_name [AS alias]`.
    fn parse_yield_item(&mut self) -> Result<YieldItem> {
        let span_start = self.current.span.start;
        if !self.is_identifier() {
            return Err(self.error("Expected field name in YIELD"));
        }
        let field_name = self.get_identifier_name();
        self.advance();
        let alias = if self.current.kind == TokenKind::As {
            self.advance();
            if !self.is_identifier() {
                return Err(self.error("Expected alias after AS"));
            }
            let alias_name = self.get_identifier_name();
            self.advance();
            Some(alias_name)
        } else {
            None
        };
        Ok(YieldItem {
            field_name,
            alias,
            span: Some(SourceSpan::new(span_start, self.current.span.start, 1, 1)),
        })
    }

    fn parse_query(&mut self) -> Result<QueryStatement> {
        let span_start = self.current.span.start;

        let mut match_clauses = Vec::new();
        let mut unwind_clauses = Vec::new();
        let mut merge_clauses = Vec::new();
        let mut create_clauses = Vec::new();
        let mut delete_clauses = Vec::new();
        let mut ordered_clauses = Vec::new();

        // Parse clauses in source order, preserving sequence for variable scoping.
        // MATCH, OPTIONAL MATCH, UNWIND, FOR, MERGE, CREATE/INSERT, DELETE can appear
        // in any order before RETURN.
        loop {
            match self.current.kind {
                TokenKind::Match | TokenKind::Optional => {
                    let clause = self.parse_match_clause()?;
                    ordered_clauses.push(QueryClause::Match(clause.clone()));
                    match_clauses.push(clause);
                }
                TokenKind::Unwind => {
                    let clause = self.parse_unwind_clause()?;
                    ordered_clauses.push(QueryClause::Unwind(clause.clone()));
                    unwind_clauses.push(clause);
                }
                TokenKind::For => {
                    let clause = self.parse_for_clause()?;
                    ordered_clauses.push(QueryClause::For(clause.clone()));
                    unwind_clauses.push(clause);
                }
                TokenKind::Merge => {
                    let clause = self.parse_merge_clause()?;
                    ordered_clauses.push(QueryClause::Merge(clause.clone()));
                    merge_clauses.push(clause);
                }
                TokenKind::Create => {
                    let clause = self.parse_create_clause_in_query()?;
                    ordered_clauses.push(QueryClause::Create(clause.clone()));
                    create_clauses.push(clause);
                }
                TokenKind::Insert => {
                    let clause = self.parse_insert()?;
                    ordered_clauses.push(QueryClause::Create(clause.clone()));
                    create_clauses.push(clause);
                }
                TokenKind::Delete | TokenKind::Detach => {
                    let clause = self.parse_delete_clause_in_query()?;
                    ordered_clauses.push(QueryClause::Delete(clause.clone()));
                    delete_clauses.push(clause);
                }
                _ if self.is_identifier()
                    && self.get_identifier_name().eq_ignore_ascii_case("LET") =>
                {
                    let bindings = self.parse_let_clause()?;
                    ordered_clauses.push(QueryClause::Let(bindings));
                }
                _ => break,
            }
        }

        // Parse WHERE or FILTER clause (after all MATCH clauses)
        let where_clause = if matches!(self.current.kind, TokenKind::Where | TokenKind::Filter) {
            Some(self.parse_where_or_filter_clause()?)
        } else {
            None
        };

        // After WHERE, allow CREATE/INSERT/DELETE/DETACH clauses
        loop {
            match self.current.kind {
                TokenKind::Create => {
                    let clause = self.parse_create_clause_in_query()?;
                    ordered_clauses.push(QueryClause::Create(clause.clone()));
                    create_clauses.push(clause);
                }
                TokenKind::Insert => {
                    let clause = self.parse_insert()?;
                    ordered_clauses.push(QueryClause::Create(clause.clone()));
                    create_clauses.push(clause);
                }
                TokenKind::Delete | TokenKind::Detach => {
                    let clause = self.parse_delete_clause_in_query()?;
                    ordered_clauses.push(QueryClause::Delete(clause.clone()));
                    delete_clauses.push(clause);
                }
                _ => break,
            }
        }

        // Parse SET clauses
        let mut set_clauses = Vec::new();
        while self.current.kind == TokenKind::Set {
            let clause = self.parse_set_clause()?;
            ordered_clauses.push(QueryClause::Set(clause.clone()));
            set_clauses.push(clause);
        }

        // Parse REMOVE clauses
        let mut remove_clauses = Vec::new();
        while self.current.kind == TokenKind::Remove {
            remove_clauses.push(self.parse_remove_clause()?);
        }

        // Parse WITH clauses
        let mut with_clauses = Vec::new();
        while self.current.kind == TokenKind::With {
            with_clauses.push(self.parse_with_clause()?);

            // After WITH, we can have more clauses
            loop {
                match self.current.kind {
                    TokenKind::Match | TokenKind::Optional => {
                        let clause = self.parse_match_clause()?;
                        ordered_clauses.push(QueryClause::Match(clause.clone()));
                        match_clauses.push(clause);
                    }
                    TokenKind::Unwind => {
                        let clause = self.parse_unwind_clause()?;
                        ordered_clauses.push(QueryClause::Unwind(clause.clone()));
                        unwind_clauses.push(clause);
                    }
                    TokenKind::For => {
                        let clause = self.parse_for_clause()?;
                        ordered_clauses.push(QueryClause::For(clause.clone()));
                        unwind_clauses.push(clause);
                    }
                    TokenKind::Merge => {
                        let clause = self.parse_merge_clause()?;
                        ordered_clauses.push(QueryClause::Merge(clause.clone()));
                        merge_clauses.push(clause);
                    }
                    _ => break,
                }
            }
        }

        // Parse RETURN, FINISH, or SELECT clause
        let return_clause = if self.current.kind == TokenKind::Return {
            self.parse_return_clause()?
        } else if self.is_identifier() && self.get_identifier_name().eq_ignore_ascii_case("FINISH")
        {
            // FINISH: consume input, return empty result
            self.advance();
            ReturnClause {
                distinct: false,
                items: Vec::new(),
                group_by: Vec::new(),
                order_by: None,
                skip: None,
                limit: None,
                is_finish: true,
                span: None,
            }
        } else if self.is_identifier() && self.get_identifier_name().eq_ignore_ascii_case("SELECT")
        {
            // SELECT: SQL-style projection, parsed as RETURN
            self.advance(); // consume SELECT
            self.parse_select_clause()?
        } else if !set_clauses.is_empty()
            || !remove_clauses.is_empty()
            || !merge_clauses.is_empty()
            || !create_clauses.is_empty()
            || !delete_clauses.is_empty()
        {
            // For mutation-only queries, return empty clause
            ReturnClause {
                distinct: false,
                items: Vec::new(),
                group_by: Vec::new(),
                order_by: None,
                skip: None,
                limit: None,
                is_finish: false,
                span: None,
            }
        } else {
            return Err(self.error("Expected RETURN, FINISH, or SELECT"));
        };

        // Parse optional HAVING clause (after RETURN, filters aggregate results)
        let having_clause = if self.current.kind == TokenKind::Having {
            Some(self.parse_having_clause()?)
        } else {
            None
        };

        Ok(QueryStatement {
            match_clauses,
            where_clause,
            set_clauses,
            remove_clauses,
            with_clauses,
            unwind_clauses,
            merge_clauses,
            create_clauses,
            delete_clauses,
            return_clause,
            having_clause,
            ordered_clauses,
            span: Some(SourceSpan::new(span_start, self.current.span.end, 1, 1)),
        })
    }

    /// Parses a FOR clause (GQL standard, ISO/IEC 39075 section 14.8).
    /// `FOR variable IN expression` — desugars to an UnwindClause.
    fn parse_for_clause(&mut self) -> Result<UnwindClause> {
        let span_start = self.current.span.start;
        self.expect(TokenKind::For)?;

        // Parse variable name
        if !self.is_identifier() {
            return Err(self.error("Expected variable name after FOR"));
        }
        let alias = self.get_identifier_name();
        self.advance();

        // Expect IN keyword
        self.expect(TokenKind::In)?;

        // Parse expression (the list to iterate)
        let expression = self.parse_expression()?;

        // Parse optional WITH ORDINALITY/OFFSET
        let (ordinality_var, offset_var) = if self.current.kind == TokenKind::With {
            self.advance(); // consume WITH
            if self.current.kind == TokenKind::Ordinality {
                self.advance(); // consume ORDINALITY
                if !self.is_identifier() {
                    return Err(self.error("Expected variable name after ORDINALITY"));
                }
                let var = self.get_identifier_name();
                self.advance();
                (Some(var), None)
            } else if self.current.kind == TokenKind::Offset {
                self.advance(); // consume OFFSET
                if !self.is_identifier() {
                    return Err(self.error("Expected variable name after OFFSET"));
                }
                let var = self.get_identifier_name();
                self.advance();
                (None, Some(var))
            } else {
                return Err(self.error("Expected ORDINALITY or OFFSET after WITH"));
            }
        } else {
            (None, None)
        };

        Ok(UnwindClause {
            expression,
            alias,
            ordinality_var,
            offset_var,
            span: Some(SourceSpan::new(span_start, self.current.span.start, 1, 1)),
        })
    }

    fn parse_set_clause(&mut self) -> Result<SetClause> {
        let span_start = self.current.span.start;
        self.expect(TokenKind::Set)?;

        let mut assignments = Vec::new();
        let mut label_operations = Vec::new();

        loop {
            // Parse variable name
            if !self.is_identifier() {
                return Err(self.error("Expected variable name in SET"));
            }
            let variable = self.current.text.clone();
            self.advance();

            // Check if this is a label operation (n:Label) or property assignment (n.prop = value)
            if self.current.kind == TokenKind::Colon {
                // Label operation: SET n:Label1:Label2
                let mut labels = Vec::new();
                while self.current.kind == TokenKind::Colon {
                    self.advance();
                    if !self.is_label_or_type_name() {
                        return Err(self.error("Expected label name after colon in SET"));
                    }
                    labels.push(self.current.text.clone());
                    self.advance();
                }
                label_operations.push(LabelOperation { variable, labels });
            } else if self.current.kind == TokenKind::Dot {
                // Property assignment: SET n.prop = value
                self.advance();

                if !self.is_label_or_type_name() {
                    return Err(self.error("Expected property name in SET"));
                }
                let property = self.current.text.clone();
                self.advance();

                self.expect(TokenKind::Eq)?;

                let value = self.parse_expression()?;

                assignments.push(PropertyAssignment {
                    variable,
                    property,
                    value,
                });
            } else {
                return Err(self.error("Expected '.' or ':' after variable in SET"));
            }

            // Check for more assignments/operations
            if self.current.kind != TokenKind::Comma {
                break;
            }
            self.advance();
        }

        Ok(SetClause {
            assignments,
            label_operations,
            span: Some(SourceSpan::new(span_start, self.current.span.end, 1, 1)),
        })
    }

    fn parse_remove_clause(&mut self) -> Result<RemoveClause> {
        let span_start = self.current.span.start;
        self.expect(TokenKind::Remove)?;

        let mut label_operations = Vec::new();
        let mut property_removals = Vec::new();

        loop {
            // Parse variable name
            if !self.is_identifier() {
                return Err(self.error("Expected variable name in REMOVE"));
            }
            let variable = self.current.text.clone();
            self.advance();

            // Check if this is a label removal (n:Label) or property removal (n.prop)
            if self.current.kind == TokenKind::Colon {
                // Label removal: REMOVE n:Label1:Label2
                let mut labels = Vec::new();
                while self.current.kind == TokenKind::Colon {
                    self.advance();
                    if !self.is_label_or_type_name() {
                        return Err(self.error("Expected label name after colon in REMOVE"));
                    }
                    labels.push(self.current.text.clone());
                    self.advance();
                }
                label_operations.push(LabelOperation { variable, labels });
            } else if self.current.kind == TokenKind::Dot {
                // Property removal: REMOVE n.prop
                self.advance();

                if !self.is_label_or_type_name() {
                    return Err(self.error("Expected property name in REMOVE"));
                }
                let property = self.current.text.clone();
                self.advance();

                property_removals.push((variable, property));
            } else {
                return Err(self.error("Expected '.' or ':' after variable in REMOVE"));
            }

            // Check for more removal operations
            if self.current.kind != TokenKind::Comma {
                break;
            }
            self.advance();
        }

        Ok(RemoveClause {
            label_operations,
            property_removals,
            span: Some(SourceSpan::new(span_start, self.current.span.end, 1, 1)),
        })
    }

    fn parse_unwind_clause(&mut self) -> Result<UnwindClause> {
        let span_start = self.current.span.start;
        self.expect(TokenKind::Unwind)?;

        // Parse the expression to unwind
        let expression = self.parse_expression()?;

        // Expect AS keyword
        self.expect(TokenKind::As)?;

        // Parse the alias
        if !self.is_identifier() {
            return Err(self.error("Expected alias after AS in UNWIND"));
        }
        let alias = self.get_identifier_name();
        self.advance();

        Ok(UnwindClause {
            expression,
            alias,
            ordinality_var: None,
            offset_var: None,
            span: Some(SourceSpan::new(span_start, self.current.span.end, 1, 1)),
        })
    }

    /// Parses `LET var = expr [, var2 = expr2]*` as a clause.
    fn parse_let_clause(&mut self) -> Result<Vec<(String, Expression)>> {
        self.advance(); // consume LET
        let mut bindings = Vec::new();
        loop {
            if !self.is_identifier() {
                return Err(self.error("Expected variable name in LET clause"));
            }
            let var = self.get_identifier_name();
            self.advance();
            self.expect(TokenKind::Eq)?;
            let expr = self.parse_expression()?;
            bindings.push((var, expr));
            if self.current.kind != TokenKind::Comma {
                break;
            }
            self.advance(); // consume comma
        }
        Ok(bindings)
    }

    fn parse_merge_clause(&mut self) -> Result<MergeClause> {
        let span_start = self.current.span.start;
        self.expect(TokenKind::Merge)?;

        // Parse the pattern to merge
        let pattern = self.parse_pattern()?;

        // Parse optional ON CREATE and ON MATCH clauses
        let mut on_create = None;
        let mut on_match = None;

        while self.current.kind == TokenKind::On {
            self.advance();

            if self.current.kind == TokenKind::Create {
                self.advance();
                self.expect(TokenKind::Set)?;
                on_create = Some(self.parse_property_assignments()?);
            } else if self.current.kind == TokenKind::Match {
                self.advance();
                self.expect(TokenKind::Set)?;
                on_match = Some(self.parse_property_assignments()?);
            } else {
                return Err(self.error("Expected CREATE or MATCH after ON in MERGE"));
            }
        }

        Ok(MergeClause {
            pattern,
            on_create,
            on_match,
            span: Some(SourceSpan::new(span_start, self.current.span.end, 1, 1)),
        })
    }

    fn parse_property_assignments(&mut self) -> Result<Vec<PropertyAssignment>> {
        let mut assignments = Vec::new();
        loop {
            // Parse variable.property = expression
            if !self.is_identifier() {
                return Err(self.error("Expected variable name"));
            }
            let variable = self.get_identifier_name();
            self.advance();

            self.expect(TokenKind::Dot)?;

            if !self.is_label_or_type_name() {
                return Err(self.error("Expected property name"));
            }
            let property = self.get_identifier_name();
            self.advance();

            self.expect(TokenKind::Eq)?;

            let value = self.parse_expression()?;

            assignments.push(PropertyAssignment {
                variable,
                property,
                value,
            });

            // Check for more assignments
            if self.current.kind != TokenKind::Comma {
                break;
            }
            self.advance();
        }

        Ok(assignments)
    }

    fn parse_match_clause(&mut self) -> Result<MatchClause> {
        let span_start = self.current.span.start;

        // Check for OPTIONAL MATCH
        let optional = if self.current.kind == TokenKind::Optional {
            self.advance();
            true
        } else {
            false
        };

        self.expect(TokenKind::Match)?;

        // Check for path mode (WALK, TRAIL, SIMPLE, ACYCLIC)
        let path_mode = match self.current.kind {
            TokenKind::Walk => {
                self.advance();
                Some(PathMode::Walk)
            }
            TokenKind::Trail => {
                self.advance();
                Some(PathMode::Trail)
            }
            TokenKind::Simple => {
                self.advance();
                Some(PathMode::Simple)
            }
            TokenKind::Acyclic => {
                self.advance();
                Some(PathMode::Acyclic)
            }
            _ => None,
        };

        // Check for match mode (DIFFERENT EDGES, REPEATABLE ELEMENTS)
        let match_mode = if self.is_identifier()
            && self.get_identifier_name().eq_ignore_ascii_case("DIFFERENT")
        {
            self.advance(); // consume DIFFERENT
            // Expect EDGES (contextual keyword)
            if self.is_identifier() && self.get_identifier_name().eq_ignore_ascii_case("EDGES") {
                self.advance();
            }
            Some(MatchMode::DifferentEdges)
        } else if self.is_identifier()
            && self
                .get_identifier_name()
                .eq_ignore_ascii_case("REPEATABLE")
        {
            self.advance(); // consume REPEATABLE
            // Expect ELEMENTS (contextual keyword)
            if self.is_identifier() && self.get_identifier_name().eq_ignore_ascii_case("ELEMENTS") {
                self.advance();
            }
            Some(MatchMode::RepeatableElements)
        } else {
            None
        };

        // Check for path search prefix (ANY, ALL SHORTEST, ANY SHORTEST, SHORTEST k)
        let search_prefix = self.parse_path_search_prefix()?;

        let mut patterns = Vec::new();
        patterns.push(self.parse_aliased_pattern()?);

        while self.current.kind == TokenKind::Comma {
            self.advance();
            patterns.push(self.parse_aliased_pattern()?);
        }

        Ok(MatchClause {
            optional,
            path_mode,
            search_prefix,
            match_mode,
            patterns,
            span: Some(SourceSpan::new(span_start, self.current.span.end, 1, 1)),
        })
    }

    /// Parses an optional path search prefix before patterns.
    ///
    /// ```text
    /// ANY [k]
    /// ALL SHORTEST
    /// ANY SHORTEST
    /// SHORTEST k [GROUPS]
    /// ```
    fn parse_path_search_prefix(&mut self) -> Result<Option<PathSearchPrefix>> {
        if self.current.kind == TokenKind::All && self.peek_kind() == TokenKind::Shortest {
            // ALL SHORTEST
            self.advance(); // consume ALL
            self.advance(); // consume SHORTEST
            return Ok(Some(PathSearchPrefix::AllShortest));
        }
        if self.is_identifier() && self.get_identifier_name().eq_ignore_ascii_case("ANY") {
            let next = self.peek_kind();
            if next == TokenKind::Shortest {
                // ANY SHORTEST
                self.advance(); // consume ANY
                self.advance(); // consume SHORTEST
                return Ok(Some(PathSearchPrefix::AnyShortest));
            }
            if next == TokenKind::Integer {
                // ANY k
                self.advance(); // consume ANY
                let k: usize = self.current.text.parse().unwrap_or(1);
                self.advance(); // consume k
                return Ok(Some(PathSearchPrefix::AnyK(k)));
            }
            if next == TokenKind::LParen {
                // ANY (pattern...) - just ANY prefix
                self.advance(); // consume ANY
                return Ok(Some(PathSearchPrefix::Any));
            }
        }
        if self.current.kind == TokenKind::Shortest {
            self.advance(); // consume SHORTEST
            if self.current.kind == TokenKind::Integer {
                let k: usize = self.current.text.parse().unwrap_or(1);
                self.advance(); // consume k
                if self.current.kind == TokenKind::Groups {
                    self.advance(); // consume GROUPS
                    return Ok(Some(PathSearchPrefix::ShortestKGroups(k)));
                }
                return Ok(Some(PathSearchPrefix::ShortestK(k)));
            }
            // SHORTEST without k: treat as SHORTEST 1
            return Ok(Some(PathSearchPrefix::ShortestK(1)));
        }
        Ok(None)
    }

    /// Parses a pattern with optional alias and path function.
    /// Supports: `p = shortestPath((a)-[*]-(b))` and `p = (a)-[*]-(b)` and `(a)-[*]-(b)`
    fn parse_aliased_pattern(&mut self) -> Result<AliasedPattern> {
        let mut alias = None;
        let mut path_function = None;

        // Check for pattern alias: identifier = ...
        if self.is_identifier() && self.peek_kind() == TokenKind::Eq {
            alias = Some(self.get_identifier_name());
            self.advance(); // consume identifier
            self.advance(); // consume =

            // Check for path function: shortestPath(...) or allShortestPaths(...)
            if self.is_identifier() {
                let func_name = self.get_identifier_name().to_lowercase();
                if func_name == "shortestpath" {
                    path_function = Some(PathFunction::ShortestPath);
                    self.advance(); // consume function name
                    self.expect(TokenKind::LParen)?;
                } else if func_name == "allshortestpaths" {
                    path_function = Some(PathFunction::AllShortestPaths);
                    self.advance(); // consume function name
                    self.expect(TokenKind::LParen)?;
                }
            }
        }

        let pattern = self.parse_pattern()?;

        if path_function.is_some() {
            self.expect(TokenKind::RParen)?;
        }

        Ok(AliasedPattern {
            alias,
            path_function,
            pattern,
        })
    }

    fn parse_with_clause(&mut self) -> Result<WithClause> {
        let span_start = self.current.span.start;
        self.expect(TokenKind::With)?;

        let distinct = if self.current.kind == TokenKind::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let mut items = Vec::new();
        items.push(self.parse_return_item()?);

        while self.current.kind == TokenKind::Comma {
            self.advance();
            items.push(self.parse_return_item()?);
        }

        // Optional WHERE after WITH
        let where_clause = if self.current.kind == TokenKind::Where {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        Ok(WithClause {
            distinct,
            items,
            where_clause,
            span: Some(SourceSpan::new(span_start, self.current.span.end, 1, 1)),
        })
    }

    fn parse_pattern(&mut self) -> Result<Pattern> {
        let node = self.parse_node_pattern()?;

        // Check for path continuation
        // Handle both `-[...]->`/`<-[...]-` style and `->` style
        if matches!(
            self.current.kind,
            TokenKind::Arrow
                | TokenKind::LeftArrow
                | TokenKind::DoubleDash
                | TokenKind::Minus
                | TokenKind::Tilde
        ) {
            let mut edges = Vec::new();

            while matches!(
                self.current.kind,
                TokenKind::Arrow
                    | TokenKind::LeftArrow
                    | TokenKind::DoubleDash
                    | TokenKind::Minus
                    | TokenKind::Tilde
            ) {
                edges.push(self.parse_edge_pattern()?);
            }

            Ok(Pattern::Path(PathPattern {
                source: node,
                edges,
                span: None,
            }))
        } else {
            Ok(Pattern::Node(node))
        }
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern> {
        self.expect(TokenKind::LParen)?;

        let variable = if self.is_identifier() {
            let name = self.get_identifier_name();
            self.advance();
            Some(name)
        } else {
            None
        };

        let mut labels = Vec::new();
        let mut label_expression = None;

        if self.current.kind == TokenKind::Is {
            // GQL IS syntax: (n IS Person | Employee)
            self.advance();
            label_expression = Some(self.parse_label_expression()?);
        } else {
            // Colon syntax: (n:Person:Employee)
            while self.current.kind == TokenKind::Colon {
                self.advance();
                if !self.is_label_or_type_name() {
                    return Err(self.error("Expected label name"));
                }
                labels.push(self.get_identifier_name());
                self.advance();
            }
        }

        // Parse properties { key: value, ... }
        let properties = if self.current.kind == TokenKind::LBrace {
            self.parse_property_map()?
        } else {
            Vec::new()
        };

        // Parse optional element pattern WHERE clause: (n WHERE n.age > 30)
        let where_clause = if self.current.kind == TokenKind::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        self.expect(TokenKind::RParen)?;

        Ok(NodePattern {
            variable,
            labels,
            label_expression,
            properties,
            where_clause,
            span: None,
        })
    }

    /// Parses a label expression with precedence: `|` < `&` < `!`.
    fn parse_label_expression(&mut self) -> Result<LabelExpression> {
        let mut left = self.parse_label_conjunction()?;

        while self.current.kind == TokenKind::Pipe {
            let mut operands = vec![left];
            while self.current.kind == TokenKind::Pipe {
                self.advance();
                operands.push(self.parse_label_conjunction()?);
            }
            left = LabelExpression::Disjunction(operands);
        }

        Ok(left)
    }

    fn parse_label_conjunction(&mut self) -> Result<LabelExpression> {
        let mut left = self.parse_label_negation()?;

        while self.current.kind == TokenKind::Ampersand {
            let mut operands = vec![left];
            while self.current.kind == TokenKind::Ampersand {
                self.advance();
                operands.push(self.parse_label_negation()?);
            }
            left = LabelExpression::Conjunction(operands);
        }

        Ok(left)
    }

    fn parse_label_negation(&mut self) -> Result<LabelExpression> {
        if self.current.kind == TokenKind::Exclamation {
            self.advance();
            let inner = self.parse_label_primary()?;
            return Ok(LabelExpression::Negation(Box::new(inner)));
        }
        self.parse_label_primary()
    }

    fn parse_label_primary(&mut self) -> Result<LabelExpression> {
        if self.current.kind == TokenKind::Percent {
            self.advance();
            return Ok(LabelExpression::Wildcard);
        }
        if self.current.kind == TokenKind::LParen {
            self.advance();
            let expr = self.parse_label_expression()?;
            self.expect(TokenKind::RParen)?;
            return Ok(expr);
        }
        if self.is_label_or_type_name() {
            let name = self.get_identifier_name();
            self.advance();
            return Ok(LabelExpression::Label(name));
        }
        Err(self.error("Expected label name, %, or ("))
    }

    fn parse_edge_pattern(&mut self) -> Result<EdgePattern> {
        // Handle both styles:
        // 1. `-[...]->` or `-[:TYPE]->` or `-[:TYPE*1..3]->` (direction determined by trailing arrow)
        // 2. `->` or `<-` or `--` (direction determined by leading arrow)

        let (variable, types, min_hops, max_hops, properties, direction) =
            if self.current.kind == TokenKind::Minus {
                // Pattern: -[...]->(target) or -[...]-(target)
                self.advance();

                // Parse [variable:TYPE*min..max {props}]
                let (var, edge_types, min_h, max_h, props) =
                    if self.current.kind == TokenKind::LBracket {
                        self.advance();

                        // Parse variable name if present
                        // Variable is followed by : (type), * (quantifier), { (properties), or ] (end)
                        let v = if self.is_identifier() {
                            let peek = self.peek_kind();
                            if matches!(
                                peek,
                                TokenKind::Colon
                                    | TokenKind::Star
                                    | TokenKind::LBrace
                                    | TokenKind::RBracket
                            ) {
                                let name = self.get_identifier_name();
                                self.advance();
                                Some(name)
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        let mut tps = Vec::new();
                        while self.current.kind == TokenKind::Colon {
                            self.advance();
                            if !self.is_label_or_type_name() {
                                return Err(self.error("Expected edge type"));
                            }
                            tps.push(self.get_identifier_name());
                            self.advance();
                            // Support pipe-separated alternatives: :T1|T2|T3
                            while self.current.kind == TokenKind::Pipe {
                                self.advance();
                                if !self.is_label_or_type_name() {
                                    return Err(self.error("Expected edge type after |"));
                                }
                                tps.push(self.get_identifier_name());
                                self.advance();
                            }
                        }

                        // Parse variable-length path quantifier: *min..max
                        let (min_h, max_h) = self.parse_path_quantifier()?;

                        // Parse edge properties: {key: value, ...}
                        let edge_props = if self.current.kind == TokenKind::LBrace {
                            self.parse_property_map()?
                        } else {
                            Vec::new()
                        };

                        self.expect(TokenKind::RBracket)?;
                        (v, tps, min_h, max_h, edge_props)
                    } else {
                        (None, Vec::new(), None, None, Vec::new())
                    };

                // Now determine direction from trailing symbol
                let dir = if self.current.kind == TokenKind::Arrow {
                    self.advance();
                    EdgeDirection::Outgoing
                } else if self.current.kind == TokenKind::Minus {
                    self.advance();
                    EdgeDirection::Undirected
                } else {
                    return Err(self.error("Expected -> or - after edge pattern"));
                };

                (var, edge_types, min_h, max_h, props, dir)
            } else if self.current.kind == TokenKind::LeftArrow {
                // Pattern: <-[...]-(target)
                self.advance();

                let (var, edge_types, min_h, max_h, props) =
                    if self.current.kind == TokenKind::LBracket {
                        self.advance();

                        // Parse variable name if present
                        // Variable is followed by : (type), * (quantifier), { (properties), or ] (end)
                        let v = if self.is_identifier() {
                            let peek = self.peek_kind();
                            if matches!(
                                peek,
                                TokenKind::Colon
                                    | TokenKind::Star
                                    | TokenKind::LBrace
                                    | TokenKind::RBracket
                            ) {
                                let name = self.get_identifier_name();
                                self.advance();
                                Some(name)
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        let mut tps = Vec::new();
                        while self.current.kind == TokenKind::Colon {
                            self.advance();
                            if !self.is_label_or_type_name() {
                                return Err(self.error("Expected edge type"));
                            }
                            tps.push(self.get_identifier_name());
                            self.advance();
                            // Support pipe-separated alternatives: :T1|T2|T3
                            while self.current.kind == TokenKind::Pipe {
                                self.advance();
                                if !self.is_label_or_type_name() {
                                    return Err(self.error("Expected edge type after |"));
                                }
                                tps.push(self.get_identifier_name());
                                self.advance();
                            }
                        }

                        // Parse variable-length path quantifier
                        let (min_h, max_h) = self.parse_path_quantifier()?;

                        // Parse edge properties: {key: value, ...}
                        let edge_props = if self.current.kind == TokenKind::LBrace {
                            self.parse_property_map()?
                        } else {
                            Vec::new()
                        };

                        self.expect(TokenKind::RBracket)?;
                        (v, tps, min_h, max_h, edge_props)
                    } else {
                        (None, Vec::new(), None, None, Vec::new())
                    };

                // Consume trailing -
                if self.current.kind == TokenKind::Minus {
                    self.advance();
                }

                (
                    var,
                    edge_types,
                    min_h,
                    max_h,
                    props,
                    EdgeDirection::Incoming,
                )
            } else if self.current.kind == TokenKind::Arrow {
                // Simple ->
                self.advance();
                (
                    None,
                    Vec::new(),
                    None,
                    None,
                    Vec::new(),
                    EdgeDirection::Outgoing,
                )
            } else if self.current.kind == TokenKind::DoubleDash {
                // Simple --
                self.advance();
                (
                    None,
                    Vec::new(),
                    None,
                    None,
                    Vec::new(),
                    EdgeDirection::Undirected,
                )
            } else if self.current.kind == TokenKind::Tilde {
                // GQL undirected edge: ~[variable:TYPE*min..max {props}]~
                self.advance();

                let (var, edge_types, min_h, max_h, props) =
                    if self.current.kind == TokenKind::LBracket {
                        self.advance();

                        let v = if self.is_identifier() {
                            let peek = self.peek_kind();
                            if matches!(
                                peek,
                                TokenKind::Colon
                                    | TokenKind::Star
                                    | TokenKind::LBrace
                                    | TokenKind::RBracket
                            ) {
                                let name = self.get_identifier_name();
                                self.advance();
                                Some(name)
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        let mut tps = Vec::new();
                        while self.current.kind == TokenKind::Colon {
                            self.advance();
                            if !self.is_label_or_type_name() {
                                return Err(self.error("Expected edge type"));
                            }
                            tps.push(self.get_identifier_name());
                            self.advance();
                            while self.current.kind == TokenKind::Pipe {
                                self.advance();
                                if !self.is_label_or_type_name() {
                                    return Err(self.error("Expected edge type after |"));
                                }
                                tps.push(self.get_identifier_name());
                                self.advance();
                            }
                        }

                        let (min_h, max_h) = self.parse_path_quantifier()?;

                        let edge_props = if self.current.kind == TokenKind::LBrace {
                            self.parse_property_map()?
                        } else {
                            Vec::new()
                        };

                        self.expect(TokenKind::RBracket)?;
                        (v, tps, min_h, max_h, edge_props)
                    } else {
                        (None, Vec::new(), None, None, Vec::new())
                    };

                // Consume trailing ~
                if self.current.kind == TokenKind::Tilde {
                    self.advance();
                }

                (
                    var,
                    edge_types,
                    min_h,
                    max_h,
                    props,
                    EdgeDirection::Undirected,
                )
            } else {
                return Err(self.error("Expected edge pattern"));
            };

        // Check for questioned edge: ->?(node) means optional (0 or 1 hop)
        let questioned = if self.current.kind == TokenKind::QuestionMark {
            self.advance();
            true
        } else {
            false
        };

        let target = self.parse_node_pattern()?;

        Ok(EdgePattern {
            variable,
            types,
            direction,
            target,
            min_hops,
            max_hops,
            properties,
            where_clause: None, // Element WHERE on edges parsed inside brackets
            questioned,
            span: None,
        })
    }

    /// Parses a path quantifier like `*`, `*2`, `*1..3`, `*..5`, `*2..`,
    /// or ISO `{m,n}`, `{m,}`, `{,n}`, `{m}`.
    /// Returns (min_hops, max_hops) where None means no quantifier was present.
    fn parse_path_quantifier(&mut self) -> Result<(Option<u32>, Option<u32>)> {
        // ISO GQL {m,n} quantifier syntax
        if self.current.kind == TokenKind::LBrace {
            self.advance(); // consume {
            if self.current.kind == TokenKind::Comma {
                // {,n}
                self.advance();
                let max_text = self.current.text.clone();
                let max: u32 = max_text
                    .parse()
                    .map_err(|_| self.error("Invalid path length"))?;
                self.advance();
                self.expect(TokenKind::RBrace)?;
                return Ok((Some(1), Some(max)));
            }
            let min_text = self.current.text.clone();
            let min: u32 = min_text
                .parse()
                .map_err(|_| self.error("Invalid path length"))?;
            self.advance();
            if self.current.kind == TokenKind::RBrace {
                // {m} means exactly m
                self.advance();
                return Ok((Some(min), Some(min)));
            }
            self.expect(TokenKind::Comma)?;
            if self.current.kind == TokenKind::RBrace {
                // {m,} means m to unbounded
                self.advance();
                return Ok((Some(min), None));
            }
            let max_text = self.current.text.clone();
            let max: u32 = max_text
                .parse()
                .map_err(|_| self.error("Invalid path length"))?;
            self.advance();
            self.expect(TokenKind::RBrace)?;
            return Ok((Some(min), Some(max)));
        }

        if self.current.kind != TokenKind::Star {
            return Ok((None, None));
        }
        self.advance(); // consume *

        // Check for bounds
        if self.current.kind == TokenKind::Integer {
            let min_text = self.current.text.clone();
            let min: u32 = min_text
                .parse()
                .map_err(|_| self.error("Invalid path length"))?;
            self.advance();

            if self.current.kind == TokenKind::Dot {
                self.advance();
                self.expect(TokenKind::Dot)?; // expect second dot for ..

                if self.current.kind == TokenKind::Integer {
                    let max_text = self.current.text.clone();
                    let max: u32 = max_text
                        .parse()
                        .map_err(|_| self.error("Invalid path length"))?;
                    self.advance();
                    Ok((Some(min), Some(max))) // *min..max
                } else {
                    Ok((Some(min), None)) // *min.. (unbounded max)
                }
            } else {
                Ok((Some(min), Some(min))) // *n means exactly n hops
            }
        } else if self.current.kind == TokenKind::Dot {
            self.advance();
            self.expect(TokenKind::Dot)?; // expect second dot for ..

            if self.current.kind == TokenKind::Integer {
                let max_text = self.current.text.clone();
                let max: u32 = max_text
                    .parse()
                    .map_err(|_| self.error("Invalid path length"))?;
                self.advance();
                Ok((Some(1), Some(max))) // *..max (min defaults to 1)
            } else {
                Err(self.error("Expected max hops after .."))
            }
        } else {
            Ok((Some(1), None)) // * alone means 1 to unbounded
        }
    }

    fn parse_where_clause(&mut self) -> Result<WhereClause> {
        self.expect(TokenKind::Where)?;
        let expression = self.parse_expression()?;

        Ok(WhereClause {
            expression,
            span: None,
        })
    }

    /// Parses either WHERE or FILTER clause (FILTER is a GQL alias for WHERE).
    fn parse_where_or_filter_clause(&mut self) -> Result<WhereClause> {
        // Accept both WHERE and FILTER
        if self.current.kind == TokenKind::Filter {
            self.advance();
        } else {
            self.expect(TokenKind::Where)?;
        }
        let expression = self.parse_expression()?;

        Ok(WhereClause {
            expression,
            span: None,
        })
    }

    fn parse_having_clause(&mut self) -> Result<HavingClause> {
        self.expect(TokenKind::Having)?;
        let expression = self.parse_expression()?;

        Ok(HavingClause {
            expression,
            span: None,
        })
    }

    fn parse_return_clause(&mut self) -> Result<ReturnClause> {
        self.expect(TokenKind::Return)?;

        let distinct = if self.current.kind == TokenKind::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let mut items = Vec::new();
        items.push(self.parse_return_item()?);

        while self.current.kind == TokenKind::Comma {
            self.advance();
            items.push(self.parse_return_item()?);
        }

        // Parse optional GROUP BY
        let group_by = if self.current.kind == TokenKind::Group {
            self.advance();
            self.expect(TokenKind::By)?;
            let mut exprs = vec![self.parse_expression()?];
            while self.current.kind == TokenKind::Comma {
                self.advance();
                exprs.push(self.parse_expression()?);
            }
            exprs
        } else {
            Vec::new()
        };

        let order_by = if self.current.kind == TokenKind::Order {
            Some(self.parse_order_by()?)
        } else {
            None
        };

        let skip = if matches!(self.current.kind, TokenKind::Skip | TokenKind::Offset) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        let limit = if self.current.kind == TokenKind::Limit {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(ReturnClause {
            distinct,
            items,
            group_by,
            order_by,
            skip,
            limit,
            is_finish: false,
            span: None,
        })
    }

    /// Parses a SELECT clause (SQL-style projection, same semantics as RETURN).
    /// Called after SELECT token has already been consumed.
    fn parse_select_clause(&mut self) -> Result<ReturnClause> {
        let distinct = if self.current.kind == TokenKind::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let mut items = Vec::new();
        items.push(self.parse_return_item()?);
        while self.current.kind == TokenKind::Comma {
            self.advance();
            items.push(self.parse_return_item()?);
        }

        // Parse optional GROUP BY
        let group_by = if self.current.kind == TokenKind::Group {
            self.advance();
            self.expect(TokenKind::By)?;
            let mut exprs = vec![self.parse_expression()?];
            while self.current.kind == TokenKind::Comma {
                self.advance();
                exprs.push(self.parse_expression()?);
            }
            exprs
        } else {
            Vec::new()
        };

        let order_by = if self.current.kind == TokenKind::Order {
            Some(self.parse_order_by()?)
        } else {
            None
        };

        let skip = if matches!(self.current.kind, TokenKind::Skip | TokenKind::Offset) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        let limit = if self.current.kind == TokenKind::Limit {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(ReturnClause {
            distinct,
            items,
            group_by,
            order_by,
            skip,
            limit,
            is_finish: false,
            span: None,
        })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem> {
        let expression = self.parse_expression()?;

        let alias = if self.current.kind == TokenKind::As {
            self.advance();
            if !self.is_identifier() {
                return Err(self.error("Expected alias name"));
            }
            let name = self.get_identifier_name();
            self.advance();
            Some(name)
        } else {
            None
        };

        Ok(ReturnItem {
            expression,
            alias,
            span: None,
        })
    }

    fn parse_order_by(&mut self) -> Result<OrderByClause> {
        self.expect(TokenKind::Order)?;
        self.expect(TokenKind::By)?;

        let mut items = Vec::new();
        items.push(self.parse_order_item()?);

        while self.current.kind == TokenKind::Comma {
            self.advance();
            items.push(self.parse_order_item()?);
        }

        Ok(OrderByClause { items, span: None })
    }

    fn parse_order_item(&mut self) -> Result<OrderByItem> {
        let expression = self.parse_expression()?;

        let order = match self.current.kind {
            TokenKind::Asc => {
                self.advance();
                SortOrder::Asc
            }
            TokenKind::Desc => {
                self.advance();
                SortOrder::Desc
            }
            _ => SortOrder::Asc,
        };

        Ok(OrderByItem { expression, order })
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_xor_expression()?;

        while self.current.kind == TokenKind::Or {
            self.advance();
            let right = self.parse_xor_expression()?;
            left = Expression::Binary {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_xor_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_and_expression()?;

        while self.current.kind == TokenKind::Xor {
            self.advance();
            let right = self.parse_and_expression()?;
            left = Expression::Binary {
                left: Box::new(left),
                op: BinaryOp::Xor,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_not_expression()?;

        while self.current.kind == TokenKind::And {
            self.advance();
            let right = self.parse_not_expression()?;
            left = Expression::Binary {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_not_expression(&mut self) -> Result<Expression> {
        if self.current.kind == TokenKind::Not {
            self.advance();
            let operand = self.parse_not_expression()?;
            return Ok(Expression::Unary {
                op: UnaryOp::Not,
                operand: Box::new(operand),
            });
        }
        self.parse_comparison_expression()
    }

    fn parse_comparison_expression(&mut self) -> Result<Expression> {
        let left = self.parse_additive_expression()?;

        // Check for regular comparison operators
        let op = match self.current.kind {
            TokenKind::Eq => Some(BinaryOp::Eq),
            TokenKind::Ne => Some(BinaryOp::Ne),
            TokenKind::Lt => Some(BinaryOp::Lt),
            TokenKind::Le => Some(BinaryOp::Le),
            TokenKind::Gt => Some(BinaryOp::Gt),
            TokenKind::Ge => Some(BinaryOp::Ge),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive_expression()?;
            return Ok(Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }

        // Check for IN, STARTS WITH, ENDS WITH, CONTAINS
        match self.current.kind {
            TokenKind::In => {
                self.advance(); // consume IN
                let right = self.parse_primary_expression()?;
                return Ok(Expression::Binary {
                    left: Box::new(left),
                    op: BinaryOp::In,
                    right: Box::new(right),
                });
            }
            TokenKind::Starts => {
                self.advance(); // consume STARTS
                self.expect(TokenKind::With)?; // expect WITH
                let right = self.parse_additive_expression()?;
                return Ok(Expression::Binary {
                    left: Box::new(left),
                    op: BinaryOp::StartsWith,
                    right: Box::new(right),
                });
            }
            TokenKind::Ends => {
                self.advance(); // consume ENDS
                self.expect(TokenKind::With)?; // expect WITH
                let right = self.parse_additive_expression()?;
                return Ok(Expression::Binary {
                    left: Box::new(left),
                    op: BinaryOp::EndsWith,
                    right: Box::new(right),
                });
            }
            TokenKind::Contains => {
                self.advance(); // consume CONTAINS
                let right = self.parse_additive_expression()?;
                return Ok(Expression::Binary {
                    left: Box::new(left),
                    op: BinaryOp::Contains,
                    right: Box::new(right),
                });
            }
            TokenKind::Is => {
                self.advance(); // consume IS
                let negated = self.current.kind == TokenKind::Not;
                if negated {
                    self.advance(); // consume NOT
                }

                let predicate = if self.current.kind == TokenKind::Null {
                    // IS [NOT] NULL
                    self.advance();
                    Expression::Unary {
                        op: if negated {
                            UnaryOp::IsNotNull
                        } else {
                            UnaryOp::IsNull
                        },
                        operand: Box::new(left),
                    }
                } else if self.is_identifier() {
                    let kw = self.get_identifier_name().to_uppercase();
                    match kw.as_str() {
                        "TYPED" => {
                            // IS [NOT] TYPED <type_name>
                            self.advance();
                            let type_name = if self.is_identifier() {
                                self.get_identifier_name().to_uppercase()
                            } else {
                                return Err(self.error("Expected type name after IS TYPED"));
                            };
                            self.advance();
                            let call = Expression::FunctionCall {
                                name: "isTyped".to_string(),
                                args: vec![left, Expression::Literal(Literal::String(type_name))],
                                distinct: false,
                            };
                            if negated {
                                Expression::Unary {
                                    op: UnaryOp::Not,
                                    operand: Box::new(call),
                                }
                            } else {
                                call
                            }
                        }
                        "DIRECTED" => {
                            // IS [NOT] DIRECTED
                            self.advance();
                            let call = Expression::FunctionCall {
                                name: "isDirected".to_string(),
                                args: vec![left],
                                distinct: false,
                            };
                            if negated {
                                Expression::Unary {
                                    op: UnaryOp::Not,
                                    operand: Box::new(call),
                                }
                            } else {
                                call
                            }
                        }
                        "LABELED" => {
                            // IS [NOT] LABELED <label>
                            self.advance();
                            let label = if self.is_identifier() {
                                self.get_identifier_name()
                            } else {
                                return Err(self.error("Expected label name after IS LABELED"));
                            };
                            self.advance();
                            let call = Expression::FunctionCall {
                                name: "hasLabel".to_string(),
                                args: vec![left, Expression::Literal(Literal::String(label))],
                                distinct: false,
                            };
                            if negated {
                                Expression::Unary {
                                    op: UnaryOp::Not,
                                    operand: Box::new(call),
                                }
                            } else {
                                call
                            }
                        }
                        "SOURCE" => {
                            // IS [NOT] SOURCE OF <variable>
                            self.advance();
                            if !(self.is_identifier()
                                && self.get_identifier_name().eq_ignore_ascii_case("OF"))
                            {
                                return Err(self.error("Expected OF after IS SOURCE"));
                            }
                            self.advance(); // consume OF
                            let var = if self.is_identifier() {
                                self.get_identifier_name()
                            } else {
                                return Err(self.error("Expected variable after IS SOURCE OF"));
                            };
                            self.advance();
                            let call = Expression::FunctionCall {
                                name: "isSource".to_string(),
                                args: vec![left, Expression::Variable(var)],
                                distinct: false,
                            };
                            if negated {
                                Expression::Unary {
                                    op: UnaryOp::Not,
                                    operand: Box::new(call),
                                }
                            } else {
                                call
                            }
                        }
                        "DESTINATION" => {
                            // IS [NOT] DESTINATION OF <variable>
                            self.advance();
                            if !(self.is_identifier()
                                && self.get_identifier_name().eq_ignore_ascii_case("OF"))
                            {
                                return Err(self.error("Expected OF after IS DESTINATION"));
                            }
                            self.advance(); // consume OF
                            let var = if self.is_identifier() {
                                self.get_identifier_name()
                            } else {
                                return Err(self.error("Expected variable after IS DESTINATION OF"));
                            };
                            self.advance();
                            let call = Expression::FunctionCall {
                                name: "isDestination".to_string(),
                                args: vec![left, Expression::Variable(var)],
                                distinct: false,
                            };
                            if negated {
                                Expression::Unary {
                                    op: UnaryOp::Not,
                                    operand: Box::new(call),
                                }
                            } else {
                                call
                            }
                        }
                        _ => {
                            return Err(self.error(
                                "Expected NULL, TYPED, DIRECTED, LABELED, SOURCE, or DESTINATION after IS",
                            ));
                        }
                    }
                } else {
                    return Err(self.error(
                        "Expected NULL, TYPED, DIRECTED, LABELED, SOURCE, or DESTINATION after IS",
                    ));
                };

                return Ok(predicate);
            }
            _ => {}
        }

        Ok(left)
    }

    fn parse_additive_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_multiplicative_expression()?;

        loop {
            let op = match self.current.kind {
                TokenKind::Plus => BinaryOp::Add,
                TokenKind::Minus => BinaryOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative_expression()?;
            left = Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_unary_expression()?;

        loop {
            let op = match self.current.kind {
                TokenKind::Star => BinaryOp::Mul,
                TokenKind::Slash => BinaryOp::Div,
                TokenKind::Percent => BinaryOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary_expression()?;
            left = Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_unary_expression(&mut self) -> Result<Expression> {
        if self.current.kind == TokenKind::Minus {
            self.advance();
            let operand = self.parse_unary_expression()?;
            return Ok(Expression::Unary {
                op: UnaryOp::Neg,
                operand: Box::new(operand),
            });
        }
        self.parse_postfix_expression()
    }

    fn parse_postfix_expression(&mut self) -> Result<Expression> {
        let mut expr = self.parse_primary_expression()?;

        loop {
            match self.current.kind {
                TokenKind::LBracket => {
                    self.advance();
                    let index = self.parse_expression()?;
                    self.expect(TokenKind::RBracket)?;
                    expr = Expression::IndexAccess {
                        base: Box::new(expr),
                        index: Box::new(index),
                    };
                }
                _ => break,
            }
        }

        Ok(expr)
    }

    fn parse_primary_expression(&mut self) -> Result<Expression> {
        match self.current.kind {
            TokenKind::Null => {
                self.advance();
                Ok(Expression::Literal(Literal::Null))
            }
            TokenKind::True => {
                self.advance();
                Ok(Expression::Literal(Literal::Bool(true)))
            }
            TokenKind::False => {
                self.advance();
                Ok(Expression::Literal(Literal::Bool(false)))
            }
            TokenKind::Integer => {
                let text = &self.current.text;
                let value = if text.starts_with("0x") || text.starts_with("0X") {
                    i64::from_str_radix(&text[2..], 16)
                } else if text.starts_with("0o") || text.starts_with("0O") {
                    i64::from_str_radix(&text[2..], 8)
                } else {
                    text.parse()
                }
                .map_err(|_| self.error("Invalid integer"))?;
                self.advance();
                Ok(Expression::Literal(Literal::Integer(value)))
            }
            TokenKind::Float => {
                let value = self
                    .current
                    .text
                    .parse()
                    .map_err(|_| self.error("Invalid float"))?;
                self.advance();
                Ok(Expression::Literal(Literal::Float(value)))
            }
            TokenKind::String => {
                let text = &self.current.text;
                let inner = &text[1..text.len() - 1]; // Remove quotes
                let value = unescape_string(inner);
                self.advance();
                Ok(Expression::Literal(Literal::String(value)))
            }
            // CASE expression - check if it's actually a CASE expression or just a variable named 'case'
            TokenKind::Case => {
                // Look ahead: if followed by WHEN, it's a CASE expression
                // If followed by : , ) AS ORDER LIMIT SKIP or EOF, it's a variable named 'case'
                let next = self.peek_kind();
                if matches!(
                    next,
                    TokenKind::Colon
                        | TokenKind::Comma
                        | TokenKind::RParen
                        | TokenKind::RBracket
                        | TokenKind::As
                        | TokenKind::Order
                        | TokenKind::Limit
                        | TokenKind::Skip
                        | TokenKind::Eof
                ) {
                    // It's a variable named 'case'
                    let name = "case".to_string();
                    self.advance();
                    Ok(Expression::Variable(name))
                } else {
                    // It's a CASE expression
                    self.parse_case_expression()
                }
            }
            // Handle type() function - must be checked BEFORE is_identifier() since TYPE is a contextual keyword
            TokenKind::Type => {
                let name = "type".to_string();
                self.advance();
                if self.current.kind != TokenKind::LParen {
                    // If not followed by (, treat as identifier/variable
                    return Ok(Expression::Variable(name));
                }
                self.advance();
                let mut args = Vec::new();
                if self.current.kind != TokenKind::RParen {
                    args.push(self.parse_expression()?);
                    while self.current.kind == TokenKind::Comma {
                        self.advance();
                        args.push(self.parse_expression()?);
                    }
                }
                self.expect(TokenKind::RParen)?;
                Ok(Expression::FunctionCall {
                    name,
                    args,
                    distinct: false,
                })
            }
            _ if self.is_identifier() => {
                let name = self.get_identifier_name();

                // LET ... IN ... END expression
                if name.eq_ignore_ascii_case("LET") {
                    self.advance(); // consume LET
                    let mut bindings = Vec::new();
                    loop {
                        if !self.is_identifier() {
                            return Err(self.error("Expected variable name in LET expression"));
                        }
                        let var = self.get_identifier_name();
                        self.advance();
                        self.expect(TokenKind::Eq)?;
                        let expr = self.parse_expression()?;
                        bindings.push((var, expr));
                        if self.current.kind != TokenKind::Comma {
                            break;
                        }
                        self.advance(); // consume comma
                    }
                    // Expect IN keyword
                    if self.current.kind != TokenKind::In {
                        return Err(self.error("Expected IN after LET bindings"));
                    }
                    self.advance(); // consume IN
                    let body = self.parse_expression()?;
                    self.expect(TokenKind::End)?;
                    return Ok(Expression::LetIn {
                        bindings,
                        body: Box::new(body),
                    });
                }

                self.advance();

                // Typed temporal literals: DATE 'str', TIME 'str', DURATION 'str', DATETIME 'str'
                if self.current.kind == TokenKind::String {
                    let upper = name.to_uppercase();
                    let make_val = |parser: &Self| {
                        let text = &parser.current.text;
                        let inner = &text[1..text.len() - 1];
                        unescape_string(inner)
                    };
                    let typed_lit = match upper.as_str() {
                        "DATE" => Some(Literal::Date(make_val(self))),
                        "TIME" => Some(Literal::Time(make_val(self))),
                        "DURATION" => Some(Literal::Duration(make_val(self))),
                        "DATETIME" => Some(Literal::Datetime(make_val(self))),
                        _ => None,
                    };
                    if let Some(lit) = typed_lit {
                        self.advance();
                        return Ok(Expression::Literal(lit));
                    }
                }

                if self.current.kind == TokenKind::Dot {
                    self.advance();
                    if !self.is_identifier() {
                        return Err(self.error("Expected property name"));
                    }
                    let property = self.get_identifier_name();
                    self.advance();
                    Ok(Expression::PropertyAccess {
                        variable: name,
                        property,
                    })
                } else if self.current.kind == TokenKind::LBrace
                    && name.eq_ignore_ascii_case("count")
                {
                    // COUNT { MATCH ... } subquery expression
                    self.advance(); // consume {
                    let inner_query = self.parse_exists_inner_query()?;
                    self.expect(TokenKind::RBrace)?;
                    Ok(Expression::CountSubquery {
                        query: Box::new(inner_query),
                    })
                } else if self.current.kind == TokenKind::LBrace
                    && name.eq_ignore_ascii_case("value")
                {
                    // VALUE { subquery } expression
                    self.advance(); // consume {
                    let inner_query = self.parse_exists_inner_query()?;
                    self.expect(TokenKind::RBrace)?;
                    Ok(Expression::ValueSubquery {
                        query: Box::new(inner_query),
                    })
                } else if self.current.kind == TokenKind::LParen {
                    // Function call
                    self.advance();
                    // Check for DISTINCT keyword in aggregate functions
                    let distinct = if self.current.kind == TokenKind::Distinct {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    let mut args = Vec::new();
                    if self.current.kind != TokenKind::RParen {
                        args.push(self.parse_expression()?);
                        while self.current.kind == TokenKind::Comma {
                            self.advance();
                            args.push(self.parse_expression()?);
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                    Ok(Expression::FunctionCall {
                        name,
                        args,
                        distinct,
                    })
                } else {
                    Ok(Expression::Variable(name))
                }
            }
            TokenKind::LParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(TokenKind::RParen)?;
                Ok(expr)
            }
            TokenKind::LBracket => {
                self.advance();
                let mut elements = Vec::new();
                if self.current.kind != TokenKind::RBracket {
                    elements.push(self.parse_expression()?);
                    while self.current.kind == TokenKind::Comma {
                        self.advance();
                        elements.push(self.parse_expression()?);
                    }
                }
                self.expect(TokenKind::RBracket)?;
                Ok(Expression::List(elements))
            }
            TokenKind::Parameter => {
                // Parameter token includes the $ prefix, so we extract just the name
                let full_text = &self.current.text;
                let name = full_text.trim_start_matches('$').to_string();
                self.advance();
                Ok(Expression::Parameter(name))
            }
            TokenKind::Exists => {
                self.advance();
                if self.current.kind == TokenKind::LBrace {
                    // EXISTS { MATCH ... } subquery form
                    self.advance(); // consume {
                    let inner_query = self.parse_exists_inner_query()?;
                    self.expect(TokenKind::RBrace)?;
                    Ok(Expression::ExistsSubquery {
                        query: Box::new(inner_query),
                    })
                } else {
                    // exists(expr) function form
                    self.expect(TokenKind::LParen)?;
                    let arg = self.parse_expression()?;
                    self.expect(TokenKind::RParen)?;
                    Ok(Expression::FunctionCall {
                        name: "exists".to_string(),
                        args: vec![arg],
                        distinct: false,
                    })
                }
            }
            TokenKind::LBrace => {
                // Map literal: {key: value, ...}
                let entries = self.parse_property_map()?;
                Ok(Expression::Map(entries))
            }
            TokenKind::Cast => {
                // CAST(expr AS type) -> desugar to toInteger/toFloat/toString
                self.advance();
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_expression()?;
                self.expect(TokenKind::As)?;
                let type_name = if self.is_identifier() {
                    let name = self.get_identifier_name().to_uppercase();
                    self.advance();
                    name
                } else {
                    return Err(self.error("Expected type name after AS"));
                };
                self.expect(TokenKind::RParen)?;
                let func_name = match type_name.as_str() {
                    "INTEGER" | "INT" | "INT64" | "BIGINT" => "toInteger",
                    "FLOAT" | "DOUBLE" | "FLOAT64" | "REAL" => "toFloat",
                    "STRING" | "VARCHAR" | "TEXT" => "toString",
                    "BOOLEAN" | "BOOL" => "toBoolean",
                    _ => return Err(self.error(&format!("Unsupported CAST type: {type_name}"))),
                };
                Ok(Expression::FunctionCall {
                    name: func_name.to_string(),
                    args: vec![expr],
                    distinct: false,
                })
            }
            _ => Err(self.error("Expected expression")),
        }
    }

    /// Parses a CASE expression.
    /// CASE [input] WHEN condition THEN result [WHEN ...] [ELSE default] END
    fn parse_case_expression(&mut self) -> Result<Expression> {
        self.expect(TokenKind::Case)?;

        // Check for simple CASE (CASE expr WHEN value THEN ...)
        // vs searched CASE (CASE WHEN condition THEN ...)
        let input = if self.current.kind != TokenKind::When {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // Parse WHEN clauses
        let mut whens = Vec::new();
        while self.current.kind == TokenKind::When {
            self.advance();
            let condition = self.parse_expression()?;
            self.expect(TokenKind::Then)?;
            let result = self.parse_expression()?;
            whens.push((condition, result));
        }

        if whens.is_empty() {
            return Err(self.error("CASE requires at least one WHEN clause"));
        }

        // Parse optional ELSE
        let else_clause = if self.current.kind == TokenKind::Else {
            self.advance();
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.expect(TokenKind::End)?;

        Ok(Expression::Case {
            input,
            whens,
            else_clause,
        })
    }

    /// Parses the inner query of an EXISTS subquery.
    /// Handles: EXISTS { MATCH (n)-[:REL]->() [WHERE ...] }
    fn parse_exists_inner_query(&mut self) -> Result<QueryStatement> {
        let mut match_clauses = Vec::new();

        // Parse MATCH clauses
        while self.current.kind == TokenKind::Match || self.current.kind == TokenKind::Optional {
            match_clauses.push(self.parse_match_clause()?);
        }

        if match_clauses.is_empty() {
            return Err(self.error("EXISTS subquery requires at least one MATCH clause"));
        }

        // Parse optional WHERE
        let where_clause = if self.current.kind == TokenKind::Where {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        // EXISTS doesn't need RETURN - create empty return clause
        Ok(QueryStatement {
            match_clauses,
            where_clause,
            set_clauses: vec![],
            remove_clauses: vec![],
            with_clauses: vec![],
            unwind_clauses: vec![],
            merge_clauses: vec![],
            create_clauses: vec![],
            delete_clauses: vec![],
            return_clause: ReturnClause {
                distinct: false,
                items: vec![],
                group_by: vec![],
                order_by: None,
                skip: None,
                limit: None,
                is_finish: false,
                span: None,
            },
            having_clause: None,
            ordered_clauses: vec![],
            span: None,
        })
    }

    fn parse_property_map(&mut self) -> Result<Vec<(String, Expression)>> {
        self.expect(TokenKind::LBrace)?;

        let mut properties = Vec::new();

        if self.current.kind != TokenKind::RBrace {
            loop {
                if !self.is_identifier() {
                    return Err(self.error("Expected property name"));
                }
                let key = self.get_identifier_name();
                self.advance();

                self.expect(TokenKind::Colon)?;

                let value = self.parse_expression()?;
                properties.push((key, value));

                if self.current.kind != TokenKind::Comma {
                    break;
                }
                self.advance();
            }
        }

        self.expect(TokenKind::RBrace)?;
        Ok(properties)
    }

    fn parse_insert(&mut self) -> Result<InsertStatement> {
        self.expect(TokenKind::Insert)?;

        let mut patterns = Vec::new();
        patterns.push(self.parse_pattern()?);

        while self.current.kind == TokenKind::Comma {
            self.advance();
            patterns.push(self.parse_pattern()?);
        }

        Ok(InsertStatement {
            patterns,
            span: None,
        })
    }

    /// Parses CREATE as INSERT (Cypher-style data modification).
    fn parse_create_as_insert(&mut self) -> Result<InsertStatement> {
        self.expect(TokenKind::Create)?;

        let mut patterns = Vec::new();
        patterns.push(self.parse_pattern()?);

        while self.current.kind == TokenKind::Comma {
            self.advance();
            patterns.push(self.parse_pattern()?);
        }

        Ok(InsertStatement {
            patterns,
            span: None,
        })
    }

    /// Parses CREATE clause within a query (e.g., MATCH ... CREATE ...).
    fn parse_create_clause_in_query(&mut self) -> Result<InsertStatement> {
        self.expect(TokenKind::Create)?;

        let mut patterns = Vec::new();
        patterns.push(self.parse_pattern()?);

        while self.current.kind == TokenKind::Comma {
            self.advance();
            patterns.push(self.parse_pattern()?);
        }

        Ok(InsertStatement {
            patterns,
            span: None,
        })
    }

    /// Parses DELETE clause within a query (e.g., MATCH ... DELETE ...).
    fn parse_delete_clause_in_query(&mut self) -> Result<DeleteStatement> {
        let detach = if self.current.kind == TokenKind::Detach {
            self.advance();
            true
        } else {
            false
        };

        self.expect(TokenKind::Delete)?;

        let mut variables = Vec::new();
        if !self.is_identifier() {
            return Err(self.error("Expected variable name in DELETE"));
        }
        variables.push(self.get_identifier_name());
        self.advance();

        while self.current.kind == TokenKind::Comma {
            self.advance();
            if !self.is_identifier() {
                return Err(self.error("Expected variable name in DELETE"));
            }
            variables.push(self.get_identifier_name());
            self.advance();
        }

        Ok(DeleteStatement {
            variables,
            detach,
            span: None,
        })
    }

    fn parse_delete(&mut self) -> Result<DeleteStatement> {
        let detach = if self.current.kind == TokenKind::Detach {
            self.advance();
            true
        } else {
            false
        };

        self.expect(TokenKind::Delete)?;

        let mut variables = Vec::new();
        if self.current.kind != TokenKind::Identifier {
            return Err(self.error("Expected variable name"));
        }
        variables.push(self.current.text.clone());
        self.advance();

        while self.current.kind == TokenKind::Comma {
            self.advance();
            if self.current.kind != TokenKind::Identifier {
                return Err(self.error("Expected variable name"));
            }
            variables.push(self.current.text.clone());
            self.advance();
        }

        Ok(DeleteStatement {
            variables,
            detach,
            span: None,
        })
    }

    fn parse_create_schema(&mut self) -> Result<SchemaStatement> {
        self.expect(TokenKind::Create)?;

        match self.current.kind {
            TokenKind::Node => {
                self.advance();
                self.expect(TokenKind::Type)?;

                if !self.is_identifier() {
                    return Err(self.error("Expected type name"));
                }
                let name = self.get_identifier_name();
                self.advance();

                // Parse property definitions
                let properties = if self.current.kind == TokenKind::LParen {
                    self.parse_property_definitions()?
                } else {
                    Vec::new()
                };

                Ok(SchemaStatement::CreateNodeType(CreateNodeTypeStatement {
                    name,
                    properties,
                    span: None,
                }))
            }
            TokenKind::Edge => {
                self.advance();
                self.expect(TokenKind::Type)?;

                if !self.is_identifier() {
                    return Err(self.error("Expected type name"));
                }
                let name = self.get_identifier_name();
                self.advance();

                let properties = if self.current.kind == TokenKind::LParen {
                    self.parse_property_definitions()?
                } else {
                    Vec::new()
                };

                Ok(SchemaStatement::CreateEdgeType(CreateEdgeTypeStatement {
                    name,
                    properties,
                    span: None,
                }))
            }
            TokenKind::Vector => {
                self.advance();
                self.expect(TokenKind::Index)?;

                // Parse index name
                if !self.is_identifier() {
                    return Err(self.error("Expected index name"));
                }
                let name = self.get_identifier_name();
                self.advance();

                // Expect ON
                self.expect(TokenKind::On)?;

                // Parse :Label(property)
                self.expect(TokenKind::Colon)?;

                if !self.is_identifier() && !self.is_label_or_type_name() {
                    return Err(self.error("Expected node label"));
                }
                let node_label = self.get_identifier_name();
                self.advance();

                self.expect(TokenKind::LParen)?;

                if !self.is_identifier() {
                    return Err(self.error("Expected property name"));
                }
                let property = self.get_identifier_name();
                self.advance();

                self.expect(TokenKind::RParen)?;

                // Parse optional DIMENSION
                let dimensions = if self.current.kind == TokenKind::Dimension {
                    self.advance();
                    if self.current.kind != TokenKind::Integer {
                        return Err(self.error("Expected integer dimension"));
                    }
                    let dim: usize = self
                        .current
                        .text
                        .parse()
                        .map_err(|_| self.error("Invalid dimension value"))?;
                    self.advance();
                    Some(dim)
                } else {
                    None
                };

                // Parse optional METRIC
                let metric = if self.current.kind == TokenKind::Metric {
                    self.advance();
                    if self.current.kind != TokenKind::String {
                        return Err(self.error("Expected metric name as string"));
                    }
                    // Remove quotes from string literal
                    let metric_str = self
                        .current
                        .text
                        .trim_matches('\'')
                        .trim_matches('"')
                        .to_string();
                    self.advance();
                    Some(metric_str)
                } else {
                    None
                };

                Ok(SchemaStatement::CreateVectorIndex(
                    CreateVectorIndexStatement {
                        name,
                        node_label,
                        property,
                        dimensions,
                        metric,
                        span: None,
                    },
                ))
            }
            _ => Err(self.error("Expected NODE, EDGE, or VECTOR")),
        }
    }

    fn parse_property_definitions(&mut self) -> Result<Vec<PropertyDefinition>> {
        self.expect(TokenKind::LParen)?;

        let mut defs = Vec::new();

        if self.current.kind != TokenKind::RParen {
            loop {
                if !self.is_identifier() {
                    return Err(self.error("Expected property name"));
                }
                let name = self.get_identifier_name();
                self.advance();

                if !self.is_identifier() {
                    return Err(self.error("Expected type name"));
                }
                let data_type = self.get_identifier_name();
                self.advance();

                let nullable = if self.current.kind == TokenKind::Not {
                    self.advance();
                    if self.current.kind != TokenKind::Null {
                        return Err(self.error("Expected NULL after NOT"));
                    }
                    self.advance();
                    false
                } else {
                    true
                };

                defs.push(PropertyDefinition {
                    name,
                    data_type,
                    nullable,
                });

                if self.current.kind != TokenKind::Comma {
                    break;
                }
                self.advance();
            }
        }

        self.expect(TokenKind::RParen)?;
        Ok(defs)
    }

    fn advance(&mut self) {
        if let Some(peeked) = self.peeked.take() {
            self.current = peeked;
        } else {
            self.current = self.lexer.next_token();
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<()> {
        if self.current.kind == kind {
            self.advance();
            Ok(())
        } else {
            Err(self.error(&format!("Expected {:?}", kind)))
        }
    }

    fn peek_kind(&mut self) -> TokenKind {
        if self.peeked.is_none() {
            self.peeked = Some(self.lexer.next_token());
        }
        self.peeked.as_ref().unwrap().kind
    }

    fn error(&self, message: &str) -> Error {
        Error::Query(
            QueryError::new(QueryErrorKind::Syntax, message)
                .with_span(self.current.span)
                .with_source(self.source.to_string()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_match() {
        let mut parser = Parser::new("MATCH (n) RETURN n");
        let result = parser.parse();
        assert!(result.is_ok());

        let stmt = result.unwrap();
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_parse_match_with_label() {
        let mut parser = Parser::new("MATCH (n:Person) RETURN n");
        let result = parser.parse();
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_match_with_where() {
        let mut parser = Parser::new("MATCH (n:Person) WHERE n.age > 30 RETURN n");
        let result = parser.parse();
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_path_pattern() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_insert() {
        let mut parser = Parser::new("INSERT (n:Person {name: 'Alice'})");
        let result = parser.parse();
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_optional_match() {
        let mut parser =
            Parser::new("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.match_clauses.len(), 2);
            assert!(!query.match_clauses[0].optional);
            assert!(query.match_clauses[1].optional);
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_with_clause() {
        let mut parser =
            Parser::new("MATCH (n:Person) WITH n.name AS name, n.age AS age RETURN name, age");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.with_clauses.len(), 1);
            assert_eq!(query.with_clauses[0].items.len(), 2);
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_order_by() {
        let mut parser = Parser::new("MATCH (n:Person) RETURN n.name ORDER BY n.age DESC");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(query) = result.unwrap() {
            let order_by = query.return_clause.order_by.as_ref().unwrap();
            assert_eq!(order_by.items.len(), 1);
            assert_eq!(order_by.items[0].order, SortOrder::Desc);
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_limit_skip() {
        let mut parser = Parser::new("MATCH (n) RETURN n SKIP 10 LIMIT 5");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(query) = result.unwrap() {
            assert!(query.return_clause.skip.is_some());
            assert!(query.return_clause.limit.is_some());
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_aggregation() {
        let mut parser = Parser::new("MATCH (n:Person) RETURN count(n), avg(n.age)");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.return_clause.items.len(), 2);
            // Check that function calls are parsed
            if let Expression::FunctionCall { name, .. } = &query.return_clause.items[0].expression
            {
                assert_eq!(name, "count");
            } else {
                panic!("Expected function call");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_with_parameter() {
        let mut parser = Parser::new("MATCH (n:Person) WHERE n.age > $min_age RETURN n");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(query) = result.unwrap() {
            // Check that the WHERE clause contains a parameter
            let where_clause = query.where_clause.as_ref().expect("Expected WHERE clause");
            if let Expression::Binary { right, .. } = &where_clause.expression {
                if let Expression::Parameter(name) = right.as_ref() {
                    assert_eq!(name, "min_age");
                } else {
                    panic!("Expected parameter, got {:?}", right);
                }
            } else {
                panic!("Expected binary expression in WHERE clause");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_insert_with_parameter() {
        let mut parser = Parser::new("INSERT (n:Person {name: $name, age: $age})");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::DataModification(DataModificationStatement::Insert(insert)) =
            result.unwrap()
        {
            if let Pattern::Node(node) = &insert.patterns[0] {
                assert_eq!(node.properties.len(), 2);
                // Check first property is a parameter
                if let Expression::Parameter(name) = &node.properties[0].1 {
                    assert_eq!(name, "name");
                } else {
                    panic!("Expected parameter for name property");
                }
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected Insert statement");
        }
    }

    #[test]
    fn test_parse_variable_length_path() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            if let Pattern::Path(path) = &query.match_clauses[0].patterns[0].pattern {
                let edge = &path.edges[0];
                assert_eq!(edge.min_hops, Some(1));
                assert_eq!(edge.max_hops, Some(3));
            } else {
                panic!("Expected path pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_variable_length_path_unbounded() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS*]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(query) = result.unwrap() {
            if let Pattern::Path(path) = &query.match_clauses[0].patterns[0].pattern {
                let edge = &path.edges[0];
                assert_eq!(edge.min_hops, Some(1)); // default min is 1
                assert_eq!(edge.max_hops, None); // unbounded max
            } else {
                panic!("Expected path pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_variable_length_path_exact() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS*2]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(query) = result.unwrap() {
            if let Pattern::Path(path) = &query.match_clauses[0].patterns[0].pattern {
                let edge = &path.edges[0];
                assert_eq!(edge.min_hops, Some(2));
                assert_eq!(edge.max_hops, Some(2)); // exact means min == max
            } else {
                panic!("Expected path pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_variable_length_path_with_properties() {
        // Test variable-length path with node properties and labels
        let query = "MATCH (start:Node {name: 'a'})-[:NEXT*1..3]->(end:Node) RETURN end.name";
        let mut parser = Parser::new(query);
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            if let Pattern::Path(path) = &query.match_clauses[0].patterns[0].pattern {
                let edge = &path.edges[0];
                assert_eq!(edge.min_hops, Some(1));
                assert_eq!(edge.max_hops, Some(3));
                // Verify source and target patterns
                assert_eq!(path.source.variable, Some("start".to_string()));
                assert_eq!(path.source.labels, vec!["Node".to_string()]);
                assert_eq!(edge.target.variable, Some("end".to_string()));
                assert_eq!(edge.target.labels, vec!["Node".to_string()]);
            } else {
                panic!("Expected path pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_reserved_keywords_as_identifiers() {
        // Test that reserved keywords can be used as variable names
        let queries = [
            ("MATCH (end:Node) RETURN end", "end"),
            ("MATCH (node:Person) RETURN node", "node"),
            ("MATCH (type:Category) RETURN type", "type"),
            ("MATCH (case:Test) RETURN case", "case"),
        ];

        for (query, expected_var) in queries {
            let mut parser = Parser::new(query);
            let result = parser.parse();
            assert!(
                result.is_ok(),
                "Parse error for '{}': {:?}",
                expected_var,
                result.err()
            );

            if let Statement::Query(q) = result.unwrap()
                && let Pattern::Node(node) = &q.match_clauses[0].patterns[0].pattern
            {
                assert_eq!(node.variable, Some(expected_var.to_string()));
            }
        }
    }

    #[test]
    fn test_parse_quoted_identifier_label() {
        let mut parser = Parser::new("MATCH (n:`rdf:type`) RETURN n");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(query) = result.unwrap() {
            if let Pattern::Node(node) = &query.match_clauses[0].patterns[0].pattern {
                assert_eq!(node.labels[0], "rdf:type");
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_unwind() {
        let mut parser = Parser::new("UNWIND [1, 2, 3] AS x RETURN x");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.unwind_clauses.len(), 1);
            assert_eq!(query.unwind_clauses[0].alias, "x");
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_merge() {
        let mut parser = Parser::new("MERGE (n:Person {name: 'Alice'}) RETURN n");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.merge_clauses.len(), 1);
            if let Pattern::Node(node) = &query.merge_clauses[0].pattern {
                assert_eq!(node.labels[0], "Person");
            } else {
                panic!("Expected node pattern in MERGE");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_merge_on_create() {
        let mut parser =
            Parser::new("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true RETURN n");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.merge_clauses.len(), 1);
            let merge = &query.merge_clauses[0];
            assert!(merge.on_create.is_some());
            assert_eq!(merge.on_create.as_ref().unwrap().len(), 1);
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_remove_label() {
        let mut parser = Parser::new("MATCH (n:Person) REMOVE n:Employee RETURN n");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.remove_clauses.len(), 1);
            assert_eq!(query.remove_clauses[0].label_operations.len(), 1);
            assert_eq!(query.remove_clauses[0].label_operations[0].variable, "n");
            assert_eq!(
                query.remove_clauses[0].label_operations[0].labels,
                vec!["Employee"]
            );
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_remove_property() {
        let mut parser = Parser::new("MATCH (n:Person) REMOVE n.age RETURN n");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.remove_clauses.len(), 1);
            assert_eq!(query.remove_clauses[0].property_removals.len(), 1);
            assert_eq!(query.remove_clauses[0].property_removals[0].0, "n");
            assert_eq!(query.remove_clauses[0].property_removals[0].1, "age");
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_remove_multiple() {
        let mut parser =
            Parser::new("MATCH (n:Person) REMOVE n:Employee, n.age, n:Contractor RETURN n");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.remove_clauses.len(), 1);
            let remove = &query.remove_clauses[0];
            // Two label operations (Employee, Contractor) and one property removal (age)
            assert_eq!(remove.label_operations.len(), 2);
            assert_eq!(remove.property_removals.len(), 1);
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_vector_function_call() {
        let mut parser = Parser::new("MATCH (n) RETURN vector([0.1, 0.2, 0.3])");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.return_clause.items.len(), 1);
            if let Expression::FunctionCall { name, args, .. } =
                &query.return_clause.items[0].expression
            {
                assert_eq!(name, "vector");
                assert_eq!(args.len(), 1);
                // The argument should be a list
                if let Expression::List(elements) = &args[0] {
                    assert_eq!(elements.len(), 3);
                } else {
                    panic!("Expected list argument, got {:?}", args[0]);
                }
            } else {
                panic!("Expected function call");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_cosine_similarity() {
        let mut parser =
            Parser::new("MATCH (n) WHERE cosine_similarity(n.embedding, $query) > 0.8 RETURN n");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            let where_clause = query.where_clause.as_ref().expect("Expected WHERE clause");
            if let Expression::Binary { left, .. } = &where_clause.expression {
                if let Expression::FunctionCall { name, args, .. } = left.as_ref() {
                    assert_eq!(name, "cosine_similarity");
                    assert_eq!(args.len(), 2);
                } else {
                    panic!("Expected function call, got {:?}", left);
                }
            } else {
                panic!("Expected binary expression");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_euclidean_distance() {
        let mut parser =
            Parser::new("MATCH (n) RETURN euclidean_distance(n.embedding, [1.0, 2.0]) AS dist");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(query) = result.unwrap() {
            assert_eq!(query.return_clause.items.len(), 1);
            if let Expression::FunctionCall { name, args, .. } =
                &query.return_clause.items[0].expression
            {
                assert_eq!(name, "euclidean_distance");
                assert_eq!(args.len(), 2);
            } else {
                panic!("Expected function call");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_create_vector_index() {
        let mut parser = Parser::new("CREATE VECTOR INDEX movie_embeddings ON :Movie(embedding)");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Schema(SchemaStatement::CreateVectorIndex(stmt)) = result.unwrap() {
            assert_eq!(stmt.name, "movie_embeddings");
            assert_eq!(stmt.node_label, "Movie");
            assert_eq!(stmt.property, "embedding");
            assert!(stmt.dimensions.is_none());
            assert!(stmt.metric.is_none());
        } else {
            panic!("Expected CreateVectorIndex statement");
        }
    }

    #[test]
    fn test_parse_create_vector_index_with_options() {
        let mut parser = Parser::new(
            "CREATE VECTOR INDEX embeddings ON :Document(vec) DIMENSION 384 METRIC 'cosine'",
        );
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Schema(SchemaStatement::CreateVectorIndex(stmt)) = result.unwrap() {
            assert_eq!(stmt.name, "embeddings");
            assert_eq!(stmt.node_label, "Document");
            assert_eq!(stmt.property, "vec");
            assert_eq!(stmt.dimensions, Some(384));
            assert_eq!(stmt.metric, Some("cosine".to_string()));
        } else {
            panic!("Expected CreateVectorIndex statement");
        }
    }

    #[test]
    fn test_in_operator_with_list() {
        let mut parser =
            Parser::new("MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob'] RETURN n.name");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(q) = result.unwrap() {
            let where_clause = q.where_clause.expect("Expected WHERE clause");
            if let WhereClause {
                expression: Expression::Binary { op, right, .. },
                ..
            } = where_clause
            {
                assert_eq!(op, BinaryOp::In);
                assert!(matches!(right.as_ref(), Expression::List(elems) if elems.len() == 2));
            } else {
                panic!("Expected Binary IN expression");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_in_operator_with_integers() {
        let mut parser = Parser::new("MATCH (n:Item) WHERE n.status IN [1, 2, 3] RETURN n");
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());
    }

    #[test]
    fn test_string_escape_single_quotes() {
        let mut parser = Parser::new(r#"MATCH (n) WHERE n.name = 'O\'Brien' RETURN n"#);
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(q) = result.unwrap() {
            let where_clause = q.where_clause.expect("Expected WHERE clause");
            if let WhereClause {
                expression: Expression::Binary { right, .. },
                ..
            } = where_clause
            {
                if let Expression::Literal(Literal::String(s)) = right.as_ref() {
                    assert_eq!(s, "O'Brien");
                } else {
                    panic!("Expected string literal");
                }
            }
        }
    }

    #[test]
    fn test_string_escape_sequences() {
        let mut parser = Parser::new(r#"MATCH (n) WHERE n.text = 'line1\nline2' RETURN n"#);
        let result = parser.parse();
        assert!(result.is_ok(), "Parse error: {:?}", result.err());

        if let Statement::Query(q) = result.unwrap() {
            let where_clause = q.where_clause.expect("Expected WHERE clause");
            if let WhereClause {
                expression: Expression::Binary { right, .. },
                ..
            } = where_clause
            {
                if let Expression::Literal(Literal::String(s)) = right.as_ref() {
                    assert_eq!(s, "line1\nline2");
                } else {
                    panic!("Expected string literal");
                }
            }
        }
    }

    // ==================== Error/Negative Cases ====================

    #[test]
    fn test_parse_error_empty_input() {
        let mut parser = Parser::new("");
        let result = parser.parse();
        assert!(result.is_err(), "Empty input should fail");
    }

    #[test]
    fn test_parse_error_just_match() {
        let mut parser = Parser::new("MATCH");
        let result = parser.parse();
        assert!(result.is_err(), "MATCH alone should fail");
    }

    #[test]
    fn test_parse_error_unclosed_node_pattern() {
        let mut parser = Parser::new("MATCH (n:Person RETURN n");
        let result = parser.parse();
        assert!(result.is_err(), "Unclosed node pattern should fail");
    }

    #[test]
    fn test_parse_error_unclosed_edge_pattern() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS->(b) RETURN a");
        let result = parser.parse();
        assert!(result.is_err(), "Unclosed edge pattern should fail");
    }

    #[test]
    fn test_parse_error_missing_return() {
        let mut parser = Parser::new("MATCH (n:Person) WHERE n.age > 25");
        let result = parser.parse();
        assert!(
            result.is_err(),
            "Query without RETURN or mutation should fail"
        );
    }

    #[test]
    fn test_parse_error_double_where() {
        let mut parser = Parser::new("MATCH (n) WHERE n.a = 1 WHERE n.b = 2 RETURN n");
        let result = parser.parse();
        assert!(result.is_err(), "Double WHERE should fail");
    }

    #[test]
    fn test_parse_error_invalid_literal() {
        let mut parser = Parser::new("MATCH (n) WHERE n.x = @invalid RETURN n");
        let result = parser.parse();
        assert!(result.is_err(), "Invalid literal should fail");
    }

    #[test]
    fn test_parse_error_unclosed_string() {
        let mut parser = Parser::new("MATCH (n) WHERE n.name = 'hello RETURN n");
        let result = parser.parse();
        assert!(result.is_err(), "Unclosed string should fail");
    }

    #[test]
    fn test_parse_error_unclosed_property_map() {
        let mut parser = Parser::new("MATCH (n:Person {name: 'Alice') RETURN n");
        let result = parser.parse();
        assert!(result.is_err(), "Unclosed property map should fail");
    }

    #[test]
    fn test_parse_error_return_only() {
        let mut parser = Parser::new("RETURN RETURN");
        let result = parser.parse();
        assert!(result.is_err(), "RETURN RETURN should fail");
    }

    #[test]
    fn test_parse_error_insert_without_pattern() {
        let mut parser = Parser::new("INSERT RETURN n");
        let result = parser.parse();
        assert!(result.is_err(), "INSERT without pattern should fail");
    }

    // ==================== 0.5.13 Features ====================

    // --- Comments ---

    #[test]
    fn test_parse_with_line_comment() {
        let mut parser = Parser::new("MATCH (n) -- find all nodes\nRETURN n");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "Line comment should be skipped: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_with_block_comment() {
        let mut parser = Parser::new("MATCH /* nodes */ (n:Person) RETURN n");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "Block comment should be skipped: {:?}",
            result.err()
        );
    }

    // --- XOR operator ---

    #[test]
    fn test_parse_xor_expression() {
        let mut parser = Parser::new("MATCH (n) WHERE n.a = 1 XOR n.b = 2 RETURN n");
        let result = parser.parse();
        assert!(result.is_ok(), "XOR should parse: {:?}", result.err());

        if let Statement::Query(q) = result.unwrap() {
            let where_clause = q.where_clause.expect("Expected WHERE clause");
            if let Expression::Binary { op, .. } = &where_clause.expression {
                assert_eq!(*op, BinaryOp::Xor);
            } else {
                panic!("Expected binary XOR expression");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    // --- ISO Path Quantifiers {m,n} ---

    #[test]
    fn test_parse_iso_path_quantifier_range() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS{2,5}]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "ISO {{m,n}} quantifier should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Pattern::Path(path) = &q.match_clauses[0].patterns[0].pattern {
                assert_eq!(path.edges[0].min_hops, Some(2));
                assert_eq!(path.edges[0].max_hops, Some(5));
            } else {
                panic!("Expected path pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_iso_path_quantifier_exact() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS{3}]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "ISO {{n}} quantifier should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Pattern::Path(path) = &q.match_clauses[0].patterns[0].pattern {
                assert_eq!(path.edges[0].min_hops, Some(3));
                assert_eq!(path.edges[0].max_hops, Some(3));
            } else {
                panic!("Expected path pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_iso_path_quantifier_lower_only() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS{2,}]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "ISO {{m,}} quantifier should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Pattern::Path(path) = &q.match_clauses[0].patterns[0].pattern {
                assert_eq!(path.edges[0].min_hops, Some(2));
                assert_eq!(path.edges[0].max_hops, None);
            } else {
                panic!("Expected path pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    // --- List Access list[i] ---

    #[test]
    fn test_parse_list_index_access() {
        let mut parser = Parser::new("MATCH (n) RETURN [1, 2, 3][0]");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "List index access should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Expression::IndexAccess { .. } = &q.return_clause.items[0].expression {
                // IndexAccess parsed
            } else {
                panic!(
                    "Expected IndexAccess expression, got {:?}",
                    q.return_clause.items[0].expression
                );
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    // --- CAST expressions ---

    #[test]
    fn test_parse_cast_to_integer() {
        let mut parser = Parser::new("MATCH (n) RETURN CAST('42' AS INTEGER)");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "CAST AS INTEGER should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Expression::FunctionCall { name, .. } = &q.return_clause.items[0].expression {
                assert_eq!(name, "toInteger");
            } else {
                panic!("Expected function call from CAST");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_cast_to_float() {
        let mut parser = Parser::new("MATCH (n) RETURN CAST(n.val AS FLOAT)");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "CAST AS FLOAT should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Expression::FunctionCall { name, .. } = &q.return_clause.items[0].expression {
                assert_eq!(name, "toFloat");
            } else {
                panic!("Expected function call from CAST");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_cast_to_string() {
        let mut parser = Parser::new("MATCH (n) RETURN CAST(42 AS STRING)");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "CAST AS STRING should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Expression::FunctionCall { name, .. } = &q.return_clause.items[0].expression {
                assert_eq!(name, "toString");
            } else {
                panic!("Expected function call from CAST");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_cast_to_boolean() {
        let mut parser = Parser::new("MATCH (n) RETURN CAST('true' AS BOOLEAN)");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "CAST AS BOOLEAN should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Expression::FunctionCall { name, .. } = &q.return_clause.items[0].expression {
                assert_eq!(name, "toBoolean");
            } else {
                panic!("Expected function call from CAST");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    // --- OFFSET as SKIP alias ---

    #[test]
    fn test_parse_offset_as_skip_alias() {
        let mut parser = Parser::new("MATCH (n) RETURN n OFFSET 10 LIMIT 5");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "OFFSET should parse as SKIP alias: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            assert!(q.return_clause.skip.is_some());
            assert!(q.return_clause.limit.is_some());
        } else {
            panic!("Expected Query statement");
        }
    }

    // --- Label Expressions (IS syntax) ---

    #[test]
    fn test_parse_is_label_single() {
        let mut parser = Parser::new("MATCH (n IS Person) RETURN n");
        let result = parser.parse();
        assert!(result.is_ok(), "IS label should parse: {:?}", result.err());

        if let Statement::Query(q) = result.unwrap() {
            if let Pattern::Node(node) = &q.match_clauses[0].patterns[0].pattern {
                match &node.label_expression {
                    Some(LabelExpression::Label(name)) => assert_eq!(name, "Person"),
                    other => panic!("Expected Label(Person), got {:?}", other),
                }
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_is_label_disjunction() {
        let mut parser = Parser::new("MATCH (n IS Person | Company) RETURN n");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "IS label disjunction should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Pattern::Node(node) = &q.match_clauses[0].patterns[0].pattern {
                assert!(matches!(
                    &node.label_expression,
                    Some(LabelExpression::Disjunction(_))
                ));
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_is_label_conjunction() {
        let mut parser = Parser::new("MATCH (n IS Person & Employee) RETURN n");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "IS label conjunction should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Pattern::Node(node) = &q.match_clauses[0].patterns[0].pattern {
                assert!(matches!(
                    &node.label_expression,
                    Some(LabelExpression::Conjunction(_))
                ));
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_is_label_negation() {
        let mut parser = Parser::new("MATCH (n IS !Inactive) RETURN n");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "IS label negation should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Pattern::Node(node) = &q.match_clauses[0].patterns[0].pattern {
                assert!(matches!(
                    &node.label_expression,
                    Some(LabelExpression::Negation(_))
                ));
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_is_label_wildcard() {
        let mut parser = Parser::new("MATCH (n IS %) RETURN n");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "IS wildcard should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Pattern::Node(node) = &q.match_clauses[0].patterns[0].pattern {
                assert!(matches!(
                    &node.label_expression,
                    Some(LabelExpression::Wildcard)
                ));
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_is_label_complex() {
        // (Person | Company) & !Inactive
        let mut parser = Parser::new("MATCH (n IS (Person | Company) & !Inactive) RETURN n");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "Complex label expression should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Pattern::Node(node) = &q.match_clauses[0].patterns[0].pattern {
                assert!(node.label_expression.is_some());
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_is_label_on_edge_colon_syntax() {
        // IS on edges is not yet wired; use colon syntax instead
        let mut parser = Parser::new("MATCH (a)-[e:KNOWS]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "Edge colon syntax should parse: {:?}",
            result.err()
        );
    }

    // --- Path Modes ---

    #[test]
    fn test_parse_path_mode_walk() {
        let mut parser = Parser::new("MATCH WALK (a)-[:KNOWS*]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(result.is_ok(), "WALK mode should parse: {:?}", result.err());

        if let Statement::Query(q) = result.unwrap() {
            assert_eq!(q.match_clauses[0].path_mode, Some(PathMode::Walk));
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_path_mode_trail() {
        let mut parser = Parser::new("MATCH TRAIL (a)-[:KNOWS*]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "TRAIL mode should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            assert_eq!(q.match_clauses[0].path_mode, Some(PathMode::Trail));
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_path_mode_simple() {
        let mut parser = Parser::new("MATCH SIMPLE (a)-[:KNOWS*]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "SIMPLE mode should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            assert_eq!(q.match_clauses[0].path_mode, Some(PathMode::Simple));
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_path_mode_acyclic() {
        let mut parser = Parser::new("MATCH ACYCLIC (a)-[:KNOWS*]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "ACYCLIC mode should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            assert_eq!(q.match_clauses[0].path_mode, Some(PathMode::Acyclic));
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_no_path_mode_default() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS*]->(b) RETURN a, b");
        let result = parser.parse();
        assert!(result.is_ok());

        if let Statement::Query(q) = result.unwrap() {
            assert_eq!(q.match_clauses[0].path_mode, None);
        } else {
            panic!("Expected Query statement");
        }
    }

    // --- Composite Queries ---

    #[test]
    fn test_parse_union() {
        let mut parser =
            Parser::new("MATCH (n:Person) RETURN n.name UNION MATCH (n:Company) RETURN n.name");
        let result = parser.parse();
        assert!(result.is_ok(), "UNION should parse: {:?}", result.err());

        if let Statement::CompositeQuery { op, .. } = result.unwrap() {
            assert_eq!(op, CompositeOp::Union);
        } else {
            panic!("Expected CompositeQuery");
        }
    }

    #[test]
    fn test_parse_union_all() {
        let mut parser =
            Parser::new("MATCH (n:Person) RETURN n.name UNION ALL MATCH (n:Company) RETURN n.name");
        let result = parser.parse();
        assert!(result.is_ok(), "UNION ALL should parse: {:?}", result.err());

        if let Statement::CompositeQuery { op, .. } = result.unwrap() {
            assert_eq!(op, CompositeOp::UnionAll);
        } else {
            panic!("Expected CompositeQuery");
        }
    }

    #[test]
    fn test_parse_except() {
        let mut parser =
            Parser::new("MATCH (n:Person) RETURN n.name EXCEPT MATCH (n:Employee) RETURN n.name");
        let result = parser.parse();
        assert!(result.is_ok(), "EXCEPT should parse: {:?}", result.err());

        if let Statement::CompositeQuery { op, .. } = result.unwrap() {
            assert_eq!(op, CompositeOp::Except);
        } else {
            panic!("Expected CompositeQuery");
        }
    }

    #[test]
    fn test_parse_intersect() {
        let mut parser = Parser::new(
            "MATCH (n:Person) RETURN n.name INTERSECT MATCH (n:Employee) RETURN n.name",
        );
        let result = parser.parse();
        assert!(result.is_ok(), "INTERSECT should parse: {:?}", result.err());

        if let Statement::CompositeQuery { op, .. } = result.unwrap() {
            assert_eq!(op, CompositeOp::Intersect);
        } else {
            panic!("Expected CompositeQuery");
        }
    }

    #[test]
    fn test_parse_otherwise() {
        let mut parser =
            Parser::new("MATCH (n:Person) RETURN n.name OTHERWISE MATCH (n:Company) RETURN n.name");
        let result = parser.parse();
        assert!(result.is_ok(), "OTHERWISE should parse: {:?}", result.err());

        if let Statement::CompositeQuery { op, .. } = result.unwrap() {
            assert_eq!(op, CompositeOp::Otherwise);
        } else {
            panic!("Expected CompositeQuery");
        }
    }

    // --- FILTER statement ---

    #[test]
    fn test_parse_filter_as_where_synonym() {
        let mut parser = Parser::new("MATCH (n:Person) FILTER n.age > 25 RETURN n");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "FILTER should parse as WHERE synonym: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            assert!(q.where_clause.is_some());
        } else {
            panic!("Expected Query statement");
        }
    }

    // --- GROUP BY ---

    #[test]
    fn test_parse_group_by() {
        let mut parser = Parser::new("MATCH (n:Person) RETURN n.city, count(n) GROUP BY n.city");
        let result = parser.parse();
        assert!(result.is_ok(), "GROUP BY should parse: {:?}", result.err());

        if let Statement::Query(q) = result.unwrap() {
            assert_eq!(q.return_clause.group_by.len(), 1);
            if let Expression::PropertyAccess { property, .. } = &q.return_clause.group_by[0] {
                assert_eq!(property, "city");
            } else {
                panic!("Expected property access in GROUP BY");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_parse_group_by_multiple() {
        let mut parser =
            Parser::new("MATCH (n:Person) RETURN n.city, n.age, count(n) GROUP BY n.city, n.age");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "Multiple GROUP BY should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            assert_eq!(q.return_clause.group_by.len(), 2);
        } else {
            panic!("Expected Query statement");
        }
    }

    // --- ELEMENT_ID function ---

    #[test]
    fn test_parse_element_id_function() {
        let mut parser = Parser::new("MATCH (n) RETURN element_id(n)");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "element_id should parse: {:?}",
            result.err()
        );

        if let Statement::Query(q) = result.unwrap() {
            if let Expression::FunctionCall { name, args, .. } =
                &q.return_clause.items[0].expression
            {
                assert_eq!(name, "element_id");
                assert_eq!(args.len(), 1);
            } else {
                panic!("Expected function call");
            }
        } else {
            panic!("Expected Query statement");
        }
    }

    // --- Error Cases for New Features ---

    #[test]
    fn test_parse_error_cast_missing_as() {
        let mut parser = Parser::new("MATCH (n) RETURN CAST(42 INTEGER)");
        let result = parser.parse();
        assert!(result.is_err(), "CAST without AS should fail");
    }

    #[test]
    fn test_parse_error_cast_invalid_type() {
        let mut parser = Parser::new("MATCH (n) RETURN CAST(42 AS VECTOR)");
        let result = parser.parse();
        assert!(result.is_err(), "CAST to unsupported type should fail");
    }

    #[test]
    fn test_parse_error_group_by_without_expressions() {
        let mut parser = Parser::new("MATCH (n) RETURN n GROUP BY");
        let result = parser.parse();
        assert!(result.is_err(), "GROUP BY without expressions should fail");
    }

    #[test]
    fn test_parse_hex_integer_literal() {
        let mut parser = Parser::new("MATCH (n) RETURN 0xFF");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "Hex literal should parse: {:?}",
            result.err()
        );
        if let Statement::Query(q) = result.unwrap() {
            let item = &q.return_clause.items[0];
            if let Expression::Literal(Literal::Integer(val)) = &item.expression {
                assert_eq!(*val, 255, "0xFF should parse to 255");
            } else {
                panic!("Expected integer literal");
            }
        }
    }

    #[test]
    fn test_parse_octal_integer_literal() {
        let mut parser = Parser::new("MATCH (n) RETURN 0o77");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "Octal literal should parse: {:?}",
            result.err()
        );
        if let Statement::Query(q) = result.unwrap() {
            let item = &q.return_clause.items[0];
            if let Expression::Literal(Literal::Integer(val)) = &item.expression {
                assert_eq!(*val, 63, "0o77 should parse to 63");
            } else {
                panic!("Expected integer literal");
            }
        }
    }

    #[test]
    fn test_parse_scientific_float_literal() {
        let mut parser = Parser::new("MATCH (n) RETURN 1.5e10");
        let result = parser.parse();
        assert!(
            result.is_ok(),
            "Scientific literal should parse: {:?}",
            result.err()
        );
        if let Statement::Query(q) = result.unwrap() {
            let item = &q.return_clause.items[0];
            if let Expression::Literal(Literal::Float(val)) = &item.expression {
                assert!((val - 1.5e10).abs() < 1.0, "1.5e10 should parse correctly");
            } else {
                panic!("Expected float literal");
            }
        }
    }

    /// Helper to extract edges from the first match pattern.
    fn get_first_path_edges(stmt: &Statement) -> &[EdgePattern] {
        if let Statement::Query(q) = stmt
            && let Pattern::Path(path) = &q.match_clauses[0].patterns[0].pattern
        {
            return &path.edges;
        }
        panic!("Expected query with path pattern");
    }

    #[test]
    fn test_parse_edge_type_pipe_alternatives() {
        let mut parser = Parser::new("MATCH (a)-[:KNOWS|LIKES|FOLLOWS]->(b) RETURN a, b");
        let result = parser.parse().expect("Edge type pipe should parse");
        let edges = get_first_path_edges(&result);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].types, vec!["KNOWS", "LIKES", "FOLLOWS"]);
    }

    #[test]
    fn test_parse_edge_type_pipe_with_variable() {
        let mut parser = Parser::new("MATCH (a)-[r:KNOWS|LIKES]->(b) RETURN r");
        let result = parser
            .parse()
            .expect("Edge type pipe with var should parse");
        let edges = get_first_path_edges(&result);
        assert_eq!(edges[0].variable, Some("r".to_string()));
        assert_eq!(edges[0].types, vec!["KNOWS", "LIKES"]);
    }

    #[test]
    fn test_parse_tilde_undirected_edge() {
        let mut parser = Parser::new("MATCH (a)~[e:KNOWS]~(b) RETURN a, b");
        let result = parser.parse().expect("Tilde edge should parse");
        let edges = get_first_path_edges(&result);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].variable, Some("e".to_string()));
        assert_eq!(edges[0].types, vec!["KNOWS"]);
        assert_eq!(edges[0].direction, EdgeDirection::Undirected);
    }

    #[test]
    fn test_parse_tilde_simple() {
        let mut parser = Parser::new("MATCH (a)~(b) RETURN a");
        let result = parser.parse().expect("Simple tilde should parse");
        let edges = get_first_path_edges(&result);
        assert_eq!(edges[0].direction, EdgeDirection::Undirected);
        assert!(edges[0].variable.is_none());
        assert!(edges[0].types.is_empty());
    }

    #[test]
    fn test_parse_tilde_with_pipe_types() {
        let mut parser = Parser::new("MATCH (a)~[:KNOWS|LIKES]~(b) RETURN a");
        let result = parser.parse().expect("Tilde with pipe types should parse");
        let edges = get_first_path_edges(&result);
        assert_eq!(edges[0].types, vec!["KNOWS", "LIKES"]);
        assert_eq!(edges[0].direction, EdgeDirection::Undirected);
    }
}
