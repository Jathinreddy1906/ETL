-- 1. Number of posts by user
SELECT u.id, u.name, COUNT(p.id) AS post_count
FROM users u
LEFT JOIN posts p ON p.user_id = u.id
GROUP BY u.id, u.name
ORDER BY post_count DESC, u.name;

-- 2. Users with more than five posts
SELECT u.name, COUNT(p.id) AS post_count
FROM users u
JOIN posts p ON p.user_id = u.id
GROUP BY u.id, u.name
HAVING COUNT(p.id) > 5
ORDER BY post_count DESC;

-- 3. Latest pipeline runs
SELECT entity_name, status, extracted_count, inserted_or_updated_count,
       rejected_count, started_at, finished_at, message
FROM pipeline_audit
ORDER BY audit_id DESC
LIMIT 20;

-- 4. Failed data-quality checks
SELECT entity_name, check_name, checked_count, failed_count, checked_at, details
FROM data_quality_checks
WHERE passed = FALSE
ORDER BY checked_at DESC;
