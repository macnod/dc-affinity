(defpackage :dc-ngram
  (:use :cl :dc-utilities :cl-ppcre)
  (:export valid-bigram-hash valid-bigram-keys valid-bigram-count
           bigram-hash bigram-vector
           valid-trigram-hash valid-trigram-keys valid-trigram-count
           trigram-hash trigram-vector normalized-trigram-vector
           affinity *valid-ngram-chars* *valid-trigram-file-name*))

(in-package :dc-ngram)

(defparameter *valid-trigram-file-name* nil)
(defparameter *valid-ngram-chars* " abcdefghijklmnopqrstuvwxyz")

(defmacro defun-with-cache (name parameters body)
  `(progn
     (defun ,name ,parameters ,body)
     (memoize ',name)))

(defun-with-cache valid-bigram-hash ()
  (loop
     with bigram-hash = (make-hash-table :test #'equal)
     for a across *valid-ngram-chars*
     do (loop for b across *valid-ngram-chars*
           for bigram = (coerce (list a b) 'string)
           when (not (string= bigram "  "))
           do (setf (gethash bigram bigram-hash) t))
     finally (return bigram-hash)))
       
(defun-with-cache valid-bigram-keys ()
  (sort (loop for k being the hash-keys in (valid-bigram-hash) collect k)
        #'string<))

(defun-with-cache valid-bigram-count () (length (valid-bigram-keys)))

(defun all-trigrams-hash ()
  (loop
     with trigram-hash = (make-hash-table :test #'equal)
     for a across *valid-ngram-chars*
     do (loop for b across *valid-ngram-chars*
           do (loop for c across *valid-ngram-chars*
                 for ngram = (coerce (list a b c) 'string)
                 when 
                   (not 
                    (or (string= "  " (subseq ngram 0 2))
                        (string= "  " (subseq ngram 1 2))))
                 do (setf
                     (gethash (coerce (list a b c) 'string) 
                              trigram-hash)
                     t)))
     finally (return trigram-hash)))

(defun-with-cache valid-trigram-hash ()
  (let ((valid-trigrams (slurp-n-thaw *valid-trigram-file-name*)))
    (loop
       with trigram-hash = (make-hash-table :test #'equal)
       for k in valid-trigrams
       do (setf (gethash k trigram-hash) t)
       finally (return trigram-hash))))


(defun-with-cache valid-trigram-keys ()
  (sort (loop for k being the hash-keys in (valid-trigram-hash) collect k)
        #'string<))

(defun-with-cache valid-trigram-count () (length (valid-trigram-keys)))

(defun bigram-hash (text)
  (when (< (length text) 2) (return-from bigram-hash nil))
  (let ((h (make-hash-table :test 'equal :size (length (valid-bigram-keys))))
        (l (string-downcase text)))
    (loop for k in (valid-bigram-keys)
       do (setf (gethash k h) 0))
    (loop for i from 0 to (- (length l) 2)
       for k = (subseq l i (+ i 2))
       when (gethash k (valid-bigram-hash))
       do (incf (gethash k h 0))
       finally (return h))))

(defun normalize-hash (hash)
  (loop
     with normalized = (make-hash-table :test 'equal
                                        :size (hash-table-count hash))
     with sum = (loop for v being the hash-values in hash summing v)
     for k being the hash-keys in hash
     do (setf (gethash k normalized)
              (float (/ (gethash k hash) sum)))
     finally (return normalized)))

(defun bigram-vector (text)
    (loop
       with h = (bigram-hash text)
       with vector  = (make-array (length (valid-bigram-keys))
                                  :element-type 'float
                                  :fill-pointer 0)
       for k in (valid-bigram-keys) do (vector-push (gethash k h 0) vector)
       finally (return vector)))

(defun trigram-hash (text)
  (when (< (length text) 3) (return-from trigram-hash nil))
  (let ((h (make-hash-table :test 'equal :size (length (valid-trigram-keys))))
        (l (string-downcase text)))
    (loop for k in (valid-trigram-keys)
         do (setf (gethash k h) 0))
    (loop for i from 0 to (- (length l) 3)
       for k = (subseq l i (+ i 3))
       when (gethash k (valid-trigram-hash))
       do (incf (gethash k h)))
    h))

(defun trigram-vector (text)
  (loop
     with h = (trigram-hash text)
     with vector  = (make-array (length (valid-trigram-keys))
                                :element-type 'float
                                :fill-pointer 0)
     for k in (valid-trigram-keys) do (vector-push (gethash k h 0) vector)
     finally (return vector)))

(defun normalized-trigram-vector (text)
  (let* ((vector  (trigram-vector text))
         (min-max (loop for a across vector
                     minimizing a into min
                     maximizing a into max
                     finally (return (list (float min) (float max)))))
         (min (first min-max))
         (max (second min-max)))
    (if (= min max)
        (return-from normalized-trigram-vector
          (map 'vector (lambda (x) (declare (ignore x)) 0.0) vector))
        (loop for x across vector
           for i = 0 then (1+ i)
           do (setf (aref vector i) (/ (- x min) (- max min)))
           finally (return vector)))))
       

(defun hash-stats (hash &optional (filter (lambda (v) (> v 0.01))))
  (loop for k being the hash-keys in hash
     for v = (gethash k hash)
     maximizing v into max
     minimizing v into min
     summing v into avg
     counting v into count
     when (funcall filter v) counting v into filtered
     finally (return (list :count count
                           :min min
                           :max max
                           :average (float (/ avg (length (hash-keys hash))))
                           :filtered filtered))))

(defun affinity (vector-a vector-b)
  (let ((a vector-a)
        (b vector-b))
    (loop for i from 0 below (length a)
       summing (* (aref a i) (aref b i)) into n
       summing (* (aref a i) (aref a i)) into sa
       summing (* (aref b i) (aref b i)) into sb
       finally (return-from affinity (/ n (* (sqrt sa) (sqrt sb)))))))
