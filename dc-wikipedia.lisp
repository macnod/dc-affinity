;; wikipedia-articles-processor.lisp
;;
;; This program extracts articles from the wiki_XX files that
;; WikiExtractor.py outputs.  The program pulls individual articles
;; from the wiki_XX files and stores the articles in a postgres
;; database.
;;
;; This program should run isolated from the dc-chattermancy
;; workspace, so load it into a separate Emacs instance if running
;; chattermancy.
;; 
(defpackage :dc-wikipedia

  (:use :cl 
        :dc-utilities 
        :cl-ppcre 
        :dc-ngram
        :sb-thread
        :gzip-stream))

(in-package :dc-wikipedia)

(defparameter *db*
  (ds `(:map
        :db "wikipedia"
        :username "chattermancy"
        :password "weasel1024"
        :host "localhost"
        :retry-count 1
        :retry-sleep 1
        :retry-sleep-factor 1
        :log-function ,(lambda (s) (format t "~a~%" s)))))

(defparameter *reference-wid* nil)
(defparameter *reference-vector* nil)
(defparameter *reference-title* nil)
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
(defparameter *article-count* nil)
(defparameter *article-count-ts* nil)
(defparameter *processing-queue* nil)

(setf dc-ngram:*valid-trigram-file-name*
      "/home/macnod/Dropbox/Code/chattermancy/lib/valid-trigrams-all.dat")

(defun article-count (&optional no-cache)
  (when (or no-cache
            (null *article-count-ts*)
            (> (- (get-universal-time) *article-count-ts*)
               (* 3600 24)))
    (setf *article-count-ts* (get-universal-time))
    (setf *article-count*
          (db-cmd *db* :query (:select (:count '*) :from 'articles) :single)))
  *article-count*)

(defun count-all-matches ()
  (db-cmd *db* :query (:select (:count '*) :from 'affinity)))

(defun title-to-wid (title)
  (db-cmd *db* :query (:select 'wid :from 'articles
                               :where (:= 'title title))
          :single))

(defun wid-to-title (wid)
  (db-cmd *db* :query (:select 'title :from 'articles :where (:= 'wid wid))
          :single))

(defun count-article-matches (title-or-wid)
  (let ((wid (if (numberp title-or-wid)
                 title-or-wid
                 (title-to-wid title-or-wid))))
    (db-cmd *db* :query
            (:select (:count '*) :from 'affinity
                     :where (:= 'aid wid))
            :single)))

(defun article-processed (title-or-wid)
  (let ((wid (if (numberp title-or-wid)
                 title-or-wid
                 (title-to-wid title-or-wid))))
    (db-cmd
     *db* :query
     (:select 1 :from 'articles
              :inner-join 'affinity
              :on (:= 'articles.wid 'affinity.aid)
              :where (:= 'articles.wid wid)) :single)))

(defun fetch-all-wids (&key (reference-wid 0) (limit (article-count)))
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

(defun compute-vector (text &key db-format)
  (let ((vector (fast-compress (trigram-vector text))))
    (if db-format
        (format nil "{~{~a~^,~}}" vector)
        vector)))
  
(defun reference-articles (&key (limit 20) formatted)
  (let* ((articles
          (db-cmd 
           *db* :query 
           (:limit 
            (:order-by 
             (:select 'wid 'preference 'title :from 'articles :where 
                      (:exists (:select 1 :from 'affinity :where 
                                        (:= 'articles.wid 'affinity.aid))))
             (:desc 'articles.preference))
            limit)))
         (format (format nil (concatenate
                              'string
                              "~~&WID~~~dt PREF~~~dtTITLE"
                              "~~{~~&~~a~~~dt~~~d<~~a~~>~~~dt~~a~~}")
                      10 17 10 5 17)))
    (if formatted
        (format t format (flatten articles))
        articles)))

(defun set-preference (title-or-wid preference)
  (let ((wid (if (numberp title-or-wid)
                 title-or-wid
                 (title-to-wid title-or-wid))))
    (db-cmd *db* :execute
            (:update 'articles :set 'preference preference
                     :where (:= 'wid title-or-wid)))))

(defun computed-articles (&optional (limit (article-count)))
  (db-cmd
   *db*
   :query
   (:limit
    (:order-by
     (:select 'a.wid 'a.preference 'a.title
              :from (:as 'articles 'a)
              :where (:exists (:select 'aid :from 'affinity
                                       :where (:= 'aid 'a.wid))))
     (:desc 'a.preference))
    limit)
   :plists))

;;
;; thread-pool compute affinity
;;
;; begin
;;

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

(defun tp-affinity (reference-wid)
  (setf *reference-wid* reference-wid
        *reference-title* (wid-to-title reference-wid)
        *reference-vector* (fetch-vector reference-wid)
        *affinity-queue* (fetch-all-wids :reference-wid reference-wid)
        *affinity-mutex* (make-mutex :name "affinity-mutex")
        *affinity* (make-array *batch-size* :fill-pointer 0)
        *smallest-score* 0
        *smallest-index* 0
        *start-time* (get-universal-time))
  (thread-pool-start :affinity *thread-count* *affinity-queue*
                     #'affinity-job))

(defun store-affinity (&key
                         (affinity *affinity*)
                         (reference-wid *reference-wid*))
  (loop for (wid score) across affinity
     do (db-cmd *db* :execute
                (:insert-into 'affinity
                 :set 'aid reference-wid 'bid wid 'score score))))

;;
;; end
;;
;; thread-pool compute affinity
;;

;; This function does a crappy job of stopping the threads that
;; contain the given text in the beginning of their name.  However,
;; it's the only thing we have right now.  You'll probably have to
;; call it twice to stop a thread-pool job.
(defun tp-stop (name)
  (let ((threads (remove-if-not 
                  (lambda (x) 
                    (scan (format nil "^~a" name)
                          (sb-thread:thread-name x)))
                  (sb-thread:list-all-threads))))
    (when threads
        (loop for thread in threads
           do (sb-thread:destroy-thread thread)
           finally (sleep 3)
             (return (sb-thread:list-all-threads))))))

(defun affinity-loop (xwids)
  (setf *affinity-store* nil)
  (let ((article-count (1- (article-count))))
    (make-thread
     (lambda ()
       (loop for xwid in xwids do
            (tp-affinity xwid)
            (sleep 60)
            (loop while (< (thread-pool-progress :affinity) article-count)
               do (sleep 60)
               finally (push (list xwid (copy-seq *affinity*))
                             *affinity-store*))))
       :name "affinity-loop")))

(defun fetch-wid-from-queue ()
  (db-cmd 
   *db* :query
   (:limit
    (:order-by 
     (:select 'a.wid :from (:as 'articles 'a)
              :inner-join (:as 'pqueue 'p) :on (:= 'a.wid 'p.wid))
     (:desc 'p.preference) 'p.id)
    1)
   :single))

(defun affinity-queue ()
  (when *processing-queue*
    (return-from affinity-queue "Already processing queue."))
  (setf *processing-queue* t)
  (let ((article-count (1- (article-count))))
    (make-thread
     (lambda ()
       (loop while *processing-queue*
          for wid = (progn (sleep 5) (fetch-wid-from-queue))
          when wid
          do
            (if (article-processed wid)
                (delete-from-queue wid)
                (progn
                  (tp-affinity wid)
                  (sleep 60)
                  (loop while (< (thread-pool-progress :affinity) article-count)
                     do (sleep 60))
                  (sleep 60)
                  (store-affinity :reference-wid wid)))
            (sb-ext:gc :foll t)))
     :name "affinity-queue")))

(defun store-results ()
  (loop for (reference-wid affinity) in *affinity-store*
     do (store-affinity :affinity affinity
                        :reference-wid reference-wid)))

(defun delete-from-queue (wid)
  (db-cmd *db* :execute (:delete-from 'pqueue :where (:= 'wid wid))))

(defun delete-from-affinity (id)
  (db-cmd *db* :execute (:delete-from 'affinity :where (:= 'id id))))


(defun fix-duplicate-matches (wid-or-title)
  (let* ((wid (if (numberp wid-or-title)
                  wid-or-title
                  (title-to-wid wid-or-title)))
         (matches (db-cmd
                   *db* :query
                   (:select 'id 'bid :from 'affinity :where (:= 'aid wid))))
         (match (make-hash-table)))
    (loop for (id bid) in matches do
         (if (gethash bid match)
             (push id (gethash bid match))
             (setf (gethash bid match) (list id))))
    (let ((ids-to-delete 
           (loop for ids being the hash-values in match
                appending (butlast ids))))
      (loop for id in ids-to-delete do (delete-from-affinity id)))))
