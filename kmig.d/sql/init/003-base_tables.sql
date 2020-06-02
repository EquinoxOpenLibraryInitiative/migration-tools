
CREATE TABLE m_borrowers LIKE borrowers;
ALTER TABLE m_borrowers ADD COLUMN x_borrowernumber INTEGER;
ALTER TABLE m_borrowers CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE m_items LIKE items;
ALTER TABLE m_items ADD COLUMN x_itemnumber INTEGER;
ALTER TABLE m_items CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE m_issues LIKE issues;
ALTER TABLE m_issues ADD COLUMN x_issue_id INTEGER;
ALTER TABLE m_issues CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE m_reserves LIKE reserves;
ALTER TABLE m_reserves ADD COLUMN x_reserve_id INTEGER;
ALTER TABLE m_reserves CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE m_accountlines LIKE accountlines;
ALTER TABLE m_accountlines ADD COLUMN x_accountlines_id INTEGER;
ALTER TABLE m_accountlines CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;


