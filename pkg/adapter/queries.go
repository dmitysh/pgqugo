package adapter

const createTaskQuery = `INSERT INTO pgqueue (kind, key, payload, attempts_left) 
		      				  VALUES ($1, $2, $3, $4)
		      			   RETURNING id`

const getWaitingTasksQuery = `WITH selected AS (
							SELECT id
							  FROM pgqueue
							 WHERE (status = 'new' OR (status IN ('retry', 'in_progress') AND next_attempt_time < now()))
							   AND kind = $1
							 ORDER BY created_at
							 LIMIT 1
						           FOR UPDATE SKIP LOCKED
						           )
							UPDATE pgqueue
							   SET status = 'in_progress', next_attempt_time = now()+$2::interval, updated_at = now()
							 WHERE id IN (SELECT id FROM selected)
						 RETURNING id, kind, key, payload, status, attempts_left-1, attempts_elapsed+1,
								   next_attempt_time, created_at, updated_at`

const succeedTasksQuery = `UPDATE pgqueue
		     				  SET status = 'succeeded', attempts_left = attempts_left-1, 
		        				  attempts_elapsed = attempts_elapsed+1, next_attempt_time = null,
		         				  updated_at = now()
						    WHERE id = ANY($1)
							  AND status = 'in_progress'`

const softFailTasksQuery = `UPDATE pgqueue
		     				   SET status = 'retry', attempts_left = attempts_left-1, 
		         				   attempts_elapsed = attempts_elapsed+1, updated_at = now()
		  					 WHERE id = ANY($1)
		    				   AND status = 'in_progress'`

const failTasksQuery = `UPDATE pgqueue
		     			   SET status = 'failed', attempts_left = 0, 
		         			   attempts_elapsed = attempts_elapsed+1, next_attempt_time = null,
		         			   updated_at = now()
					     WHERE id = ANY($1)
						   AND status = 'in_progress'`
