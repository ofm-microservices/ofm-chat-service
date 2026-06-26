package scylla

const (
	insertChatQuery = `
		INSERT INTO chats_by_order (order_id, buyer_id, seller_id, status, close_reason, last_message_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		IF NOT EXISTS
	`

	getChatByOrderIDQuery = `
		SELECT order_id, buyer_id, seller_id, status, close_reason, last_message_at, created_at, updated_at
		FROM chats_by_order
		WHERE order_id = ?
		LIMIT 1
	`

	closeChatQuery = `
		UPDATE chats_by_order
		SET status = ?, close_reason = ?, updated_at = ?
		WHERE order_id = ?
	`

	touchChatLastMessageAtQuery = `
		UPDATE chats_by_order
		SET last_message_at = ?, updated_at = ?
		WHERE order_id = ?
	`

	insertMessageByOrderQuery = `
		INSERT INTO chat_messages_by_order (order_id, created_at, message_id, sender_user_id, message_type, ciphertext, has_text, deleted, edited, attachments_json, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	insertMessageByIDQuery = `
		INSERT INTO chat_messages_by_id (order_id, message_id, sender_user_id, message_type, ciphertext, has_text, deleted, edited, attachments_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	getMessageByIDQuery = `
		SELECT order_id, message_id, sender_user_id, message_type, ciphertext, has_text, deleted, edited, attachments_json, created_at, updated_at
		FROM chat_messages_by_id
		WHERE order_id = ? AND message_id = ?
		LIMIT 1
	`

	updateMessageByIDQuery = `
		UPDATE chat_messages_by_id
		SET ciphertext = ?, has_text = ?, edited = ?, updated_at = ?
		WHERE order_id = ? AND message_id = ?
	`

	updateMessageByOrderQuery = `
		UPDATE chat_messages_by_order
		SET ciphertext = ?, has_text = ?, edited = ?, updated_at = ?
		WHERE order_id = ? AND created_at = ? AND message_id = ?
	`

	deleteMessageByIDQuery = `
		UPDATE chat_messages_by_id
		SET deleted = ?, updated_at = ?
		WHERE order_id = ? AND message_id = ?
	`

	deleteMessageByOrderQuery = `
		UPDATE chat_messages_by_order
		SET deleted = ?, updated_at = ?
		WHERE order_id = ? AND created_at = ? AND message_id = ?
	`

	listMessagesByOrderQuery = `
		SELECT order_id, message_id, sender_user_id, message_type, ciphertext, has_text, deleted, edited, attachments_json, created_at, updated_at
		FROM chat_messages_by_order
		WHERE order_id = ?
		LIMIT ?
	`

	listMessagesByOrderBeforeQuery = `
		SELECT order_id, message_id, sender_user_id, message_type, ciphertext, has_text, deleted, edited, attachments_json, created_at, updated_at
		FROM chat_messages_by_order
		WHERE order_id = ? AND (created_at, message_id) < (?, ?)
		LIMIT ?
	`
)
