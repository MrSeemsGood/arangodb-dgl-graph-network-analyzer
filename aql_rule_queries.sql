/*
    Экспертное правило 2:
    за час не может быть совершено более 3-х транзакций между одним и тем же получателем и отправителем.
*/

FOR tx IN transaction
    COLLECT sender = tx.sender, receiver = tx.receiver, dttm = tx.timestamp
    INTO txGroup
    
    LET txGroupSize = LENGTH(txGroup)
    
    FILTER txGroupSize > 3
    
    LET alerted_tx_ids = txGroup[*].tx.tx_id
    
FOR tx IN transaction
    FILTER tx.tx_id IN alerted_tx_ids
    
    UPDATE tx WITH {is_alerted : true} IN transaction
;

/*
    Экспертное правило 3:
    операция совершается с аккаунта, созданного в тот же час или за час до совершения операции.
*/

FOR acc IN account
    FOR v_acc, e_tx
    IN 1..1 ANY acc transaction
        FILTER (e_tx.timestamp - acc.account_creation_dttm) <= 1
            
        UPDATE e_tx WITH {is_alerted : true} IN transaction
;