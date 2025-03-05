import re
from pymongo import MongoClient

def print_table(title, headers, rows):
    # Calculate maximum width per column
    col_widths = [max(len(str(val)) for val in [header] + [row[i] for row in rows]) for i, header in enumerate(headers)]
    print(f"\n{title}")
    header_line = "| " + " | ".join(header.ljust(col_widths[i]) for i, header in enumerate(headers)) + " |"
    separator = "|-" + "-|-".join('-' * col_widths[i] for i in range(len(headers))) + "-|"
    print(header_line)
    print(separator)
    for row in rows:
        print("| " + " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row))) + " |")
    print(f"Total count: {len(rows)}\n")

# Database connection info


client = MongoClient(
    host=target_host,
    port=target_port,
    username=target_username,
    password=target_password,
    authSource=target_auth_db
)
db = client[target_db_name]

# Regex for "test" (case-insensitive)
test_regex = re.compile("test", re.IGNORECASE)

########################################
# 1. RESUMES collection (top-level key: resumeId)
########################################
resumes_collection = db["resumes"]
resumes_to_delete = list(resumes_collection.find({"resumeId": {"$regex": test_regex}}, {"_id": 1, "resumeId": 1}))
if resumes_to_delete:
    rows = [[str(doc["_id"]), doc["resumeId"]] for doc in resumes_to_delete]
    print_table("Resumes to Delete (Top-level):", ["_id", "resumeId"], rows)
else:
    print("\nNo resumes found with 'test' in resumeId.")

resumes_result = resumes_collection.delete_many({"resumeId": {"$regex": test_regex}})
print(f"Deleted {resumes_result.deleted_count} documents from 'resumes' collection.")

########################################
# 2. RESUME_MATCHES collection 
#    - Top-level key: resumeId (delete entire document if it contains "test")
#    - Nested key: jobId inside matches array (remove only matching nested objects)
########################################
resume_matches_collection = db["resume_matches"]

# a) Top-level deletion
rm_to_delete = list(resume_matches_collection.find({"resumeId": {"$regex": test_regex}}, {"_id": 1, "resumeId": 1}))
if rm_to_delete:
    rows = [[str(doc["_id"]), doc["resumeId"]] for doc in rm_to_delete]
    print_table("Resume_Matches to Delete (Top-level):", ["_id", "resumeId"], rows)
else:
    print("\nNo resume_matches found with 'test' in resumeId at top-level.")

rm_top_result = resume_matches_collection.delete_many({"resumeId": {"$regex": test_regex}})
print(f"Deleted {rm_top_result.deleted_count} documents from 'resume_matches' collection (top-level).")

# b) Nested removal: in remaining resume_matches, remove only nested objects in "matches" where jobId contains "test"
nested_rm = []
for doc in resume_matches_collection.find({"matches.jobId": {"$regex": test_regex}}, {"_id": 1, "matches": 1}):
    for m in doc.get("matches", []):
        if "jobId" in m and test_regex.search(m["jobId"]):
            nested_rm.append([str(doc["_id"]), m["jobId"]])
if nested_rm:
    print_table("Nested 'jobId' to Remove in resume_matches:", ["Parent _id", "jobId"], nested_rm)
else:
    print("\nNo nested jobId found with 'test' in resume_matches.")

rm_nested_result = resume_matches_collection.update_many(
    {"matches.jobId": {"$regex": test_regex}},
    {"$pull": {"matches": {"jobId": {"$regex": test_regex}}}}
)
print(f"Modified {rm_nested_result.modified_count} resume_matches documents for nested removal.")

########################################
# 3. JOB_DESCRIPTION collection (top-level key: jobId)
########################################
job_desc_collection = db["job_description"]
jd_to_delete = list(job_desc_collection.find({"jobId": {"$regex": test_regex}}, {"_id": 1, "jobId": 1}))
if jd_to_delete:
    rows = [[str(doc["_id"]), doc["jobId"]] for doc in jd_to_delete]
    print_table("Job_Description to Delete (Top-level):", ["_id", "jobId"], rows)
else:
    print("\nNo job_description found with 'test' in jobId.")

jd_result = job_desc_collection.delete_many({"jobId": {"$regex": test_regex}})
print(f"Deleted {jd_result.deleted_count} documents from 'job_description' collection.")

########################################
# 4. MATCHES collection 
#    - Top-level key: jobId (delete entire document if it contains "test")
#    - Nested key: resumeId inside matches array (remove only matching nested objects)
########################################
matches_collection = db["matches"]

# a) Top-level deletion
matches_to_delete = list(matches_collection.find({"jobId": {"$regex": test_regex}}, {"_id": 1, "jobId": 1}))
if matches_to_delete:
    rows = [[str(doc["_id"]), doc["jobId"]] for doc in matches_to_delete]
    print_table("Matches to Delete (Top-level):", ["_id", "jobId"], rows)
else:
    print("\nNo matches found with 'test' in jobId at top-level.")

matches_top_result = matches_collection.delete_many({"jobId": {"$regex": test_regex}})
print(f"Deleted {matches_top_result.deleted_count} documents from 'matches' collection (top-level).")

# b) Nested removal: in remaining matches, remove only nested objects where resumeId contains "test"
nested_matches = []
for doc in matches_collection.find({"matches.resumeId": {"$regex": test_regex}}, {"_id": 1, "matches": 1}):
    for m in doc.get("matches", []):
        if "resumeId" in m and test_regex.search(m["resumeId"]):
            nested_matches.append([str(doc["_id"]), m["resumeId"]])
if nested_matches:
    print_table("Nested 'resumeId' to Remove in matches:", ["Parent _id", "resumeId"], nested_matches)
else:
    print("\nNo nested resumeId found with 'test' in matches.")

matches_nested_result = matches_collection.update_many(
    {"matches.resumeId": {"$regex": test_regex}},
    {"$pull": {"matches": {"resumeId": {"$regex": test_regex}}}}
)
print(f"Modified {matches_nested_result.modified_count} matches documents for nested removal.")

client.close()
