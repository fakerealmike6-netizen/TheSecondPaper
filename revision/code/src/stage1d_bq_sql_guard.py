"""Lexical masking for finite read-only BigQuery dry-run validation."""


def sql_code(sql):
    """Keep executable tokens; mask comments, quoted strings and identifiers.

    The caller still verifies referenced tables against the original SQL and
    still rejects multiple statements, DML, DDL and external-query operations.
    This is lexical validation, not a SQL compiler or an execution capability.
    """
    result=[]; i=0; size=len(sql)
    while i<size:
        if sql.startswith('--',i):
            end=sql.find('\n',i+2); end=size if end<0 else end
            result.append(' '); i=end; continue
        if sql.startswith('/*',i):
            end=sql.find('*/',i+2)
            if end<0: raise ValueError('Unterminated SQL comment')
            result.append(' '); i=end+2; continue
        quote=sql[i]
        if quote in ("'", '"', '`'):
            delimiter=quote*3 if quote!='`' and sql.startswith(quote*3,i) else quote
            end=i+len(delimiter)
            while end<size:
                if sql[end]=='\\': end+=2; continue
                if sql.startswith(delimiter,end):
                    if len(delimiter)==1 and quote!='`' and sql.startswith(quote*2,end):
                        end+=2; continue
                    end+=len(delimiter); break
                end+=1
            else: raise ValueError('Unterminated SQL literal or identifier')
            if end>size: raise ValueError('Unterminated SQL literal escape')
            result.append(' '); i=end; continue
        result.append(sql[i]); i+=1
    return ''.join(result)
