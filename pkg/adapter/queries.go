package adapter

const createTaskQuery = `INSERT INTO pgqueue (kind, key, payload, attempts_left, next_attempt_time) 
		      			 VALUES ($1, $2, $3, $4, now())
		      		ON CONFLICT DO NOTHING
		      	      RETURNING id`

const getPendingTasksQuery = `WITH selected AS (
							SELECT id
							  FROM pgqueue
							 WHERE kind = $1
							   AND status IN ('new', 'in_progress', 'retry')
						       AND next_attempt_time < now()
						  ORDER BY created_at
							 LIMIT $2
                 FOR NO KEY UPDATE SKIP LOCKED)

							UPDATE pgqueue
							   SET status = 'in_progress', next_attempt_time = now()+$3::interval, updated_at = now()
							 WHERE id IN (SELECT id FROM selected)
						 RETURNING id, key, created_at, updated_at, next_attempt_time, payload,
								   status, kind, attempts_left-1, attempts_elapsed+1`

const succeedTaskQuery = `UPDATE pgqueue
		     				 SET status = 'succeeded', attempts_left = attempts_left-1, 
		        			     attempts_elapsed = attempts_elapsed+1, next_attempt_time = null,
		         			     updated_at = now()
						   WHERE id = $1
							 AND status = 'in_progress'`

const softFailTaskQuery = `UPDATE pgqueue
		     				  SET status = 'retry', attempts_left = attempts_left-1, 
		         				  attempts_elapsed = attempts_elapsed+1, updated_at = now(),
		     				      next_attempt_time = now()+$2::interval
		  				    WHERE id = $1
		    				  AND status = 'in_progress'`

const failTaskQuery = `UPDATE pgqueue
		     			  SET status = 'failed', attempts_left = 0, 
		         			  attempts_elapsed = attempts_elapsed+1, next_attempt_time = null,
		         			  updated_at = now()
					    WHERE id = $1
						  AND status = 'in_progress'`

const cleanTerminalTasksQuery = `WITH selected AS (
							   SELECT id
								 FROM pgqueue
							    WHERE kind = $1
								  AND status IN ('failed', 'succeeded')
								  AND updated_at < now() - $2::interval
							 ORDER BY updated_at
							    LIMIT $3
                           FOR UPDATE SKIP LOCKED)

						  DELETE FROM pgqueue
								WHERE id IN (SELECT id FROM selected)`

const registerJobsQuery = `INSERT INTO pgqueue_job (name) 
						   VALUES ($1)
					  ON CONFLICT (name) DO NOTHING`

const executeJobQuery = `UPDATE pgqueue_job 
						    SET updated_at = now() 
						  WHERE name = $1 
					 	    AND updated_at <= now() - $2::interval 
					  RETURNING true`
