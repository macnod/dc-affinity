(asdf:defsystem :dc-wikipedia

    :depends-on (:cl-ppcre
                 :dc-utilities
                 :dc-ngram
                 :gzip-stream)

    :components ((:file "dc-wikipedia")))
