(defpackage :dc-wikipedia-a
  (:use :cl
        :dc-utilities
        :cl-ppcre
        :dc-ngram
        :sb-thread))

(defparameter *reference-wid* nil)
(defparameter *reference-vector* nil)
(defparameter *affinity-queue* nil)
(defparameter *affinity* nil)
(defparameter *smallest-score* 0.0)
(defparameter *smallest-index* nil)
(defparameter *affinity-mutex* nil)
(defparameter *batch-size* 1000)
(defparameter *start-time* nil)
(defparameter *run-time* nil)
(defparameter *thread-count* 8)
(defparameter *affinity-store* nil)
(defparameter *wids* nil)

(defun fast-decompress (l)
  (map 'vector 'identity
       (if (listp l)
           (loop for n in l appending
                (if (< n 0) (loop for a from 1 to (- n) collect 0) (list n)))
           (loop for n across l appending
                (if (< n 0) (loop for a from 1 to (- n) collect 0) (list n))))))

(defun fetch-all-wids (&key (reference-wid 0) (limit 4078968))
  (db-cmd
   *db* :query
   (:limit
    (:order-by
     (:select 'wid :from 'articles
              :where (:<> 'wid reference-wid))
     'wid)
    limit)
    :column))

(defun fetch-vector (wid)
  (fast-decompress
   (db-cmd *db* :query (:select 'cvector :from 'articles :where (:= 'wid wid))
           :single)))

(defun set-smallest ()
  (unless (zerop (length *affinity*))
    (loop with smallest-index = 0
       with smallest-score = 1.0
       for index from 0 below (length *affinity*)
       for current-score = (second (aref *affinity* index))
       when (< current-score smallest-score)
       do (setf smallest-index index)
         (setf smallest-score current-score)
       finally (progn
                 (setf *smallest-index* smallest-index)
                 (setf *smallest-score* smallest-score)))))

(defun affinity-job (wid)
  (let* ((vector (fetch-vector wid))
         (affinity (affinity *reference-vector* vector)))
    (with-mutex (*affinity-mutex*)
      (if (< (length *affinity*) *batch-size*)
          (progn
            (vector-push-extend (list wid affinity) *affinity*)
            (when (= (length *affinity*) *batch-size*)
              (set-smallest)))
          (when (> affinity *smallest-score*)
            (setf (aref *affinity* *smallest-index*)
                  (list wid affinity))
            (set-smallest))))))

(defun affinity-finally ()
  (setf *run-time* (- (get-universal-time) *start-time*)))

(defun tp-affinity (reference-wid)
  (setf *reference-wid* reference-wid
        *reference-vector* (fetch-vector reference-wid)
        *affinity-queue* (fetch-all-wids :reference-wid reference-wid)
        *affinity-mutex* (make-mutex :name "affinity-mutex")
        *affinity* (make-array *batch-size* :fill-pointer 0)
        *smallest-score* 0
        *smallest-index* 0
        *start-time* (get-universal-time))
  (start-thread-pool "affinity" *thread-count* *affinity-queue*
                     #'affinity-job #'affinity-finally))

(defun affinity-loop (xwids)
  (setf *affinity-store* nil)
  (make-thread
   (lambda ()
     (loop for xwid in xwids do
          (tp-affinity xwid)
          (sleep 60)
          (loop while (< *dc-thread-pool-progress* 4078967) do (sleep 60)
             finally (push (list xwid (copy-seq *affinity*)) *affinity-store*))))
   :name "affinity-loop"))


(defun store-results ()
  (loop for (reference-wid affinity) in *affinity-store*
     do (store-affinity :affinity affinity
                        :reference-wid reference-wid)))

(defun store-affinity (&key
                         (affinity *affinity*)
                         (reference-wid *reference-wid*))
  (loop for (wid score) across affinity
     do (db-cmd *db* :execute
                (:insert-into 'affinity
                 :set 'aid reference-wid 'bid wid 'score score))))

