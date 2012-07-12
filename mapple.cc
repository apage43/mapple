#include <iostream>
#include <string>
#include <sstream>
#include <map>
#include <list>
#include <utility>
#include <libcouchstore/couch_db.h>
#include <glob.h>
#include <stdint.h>
#include "cJSON.h"
#include "mapreduce.h"

using std::cerr;
using std::endl;
using std::string;
using std::map;
using std::pair;

typedef enum {
    ACTIVE,
    REPLICA,
    PENDING,
    DEAD,
    UNKNOWN
} vb_state;

struct vbinfo {
    vb_state state;
    //uint64_t checkpoint_id;
    //uint64_t max_deleted_seqno;
};

int parseVBState(const string& source, vbinfo* vi) {
    int errcode = 0;
    cJSON* vbstateJ = cJSON_Parse(source.c_str());
    if(!vbstateJ) {
        return -1;
    }
    cJSON* stateJ = cJSON_GetObjectItem(vbstateJ, "state");
    if(!stateJ) {
        errcode = -2;
    } else {
        if(strcmp("active", stateJ->valuestring) == 0) {
            vi->state = ACTIVE;
        } else if (strcmp("replica", stateJ->valuestring) == 0) {
            vi->state = REPLICA;
        } else if (strcmp("pending", stateJ->valuestring) == 0) {
            vi->state = PENDING;
        } else if (strcmp("dead", stateJ->valuestring) == 0) {
            vi->state = DEAD;
        } else {
            vi->state = UNKNOWN;
        }

    }

    cJSON_Delete(vbstateJ);
    return errcode;
}

struct DesignDoc {
    DesignDoc(const string& source) {
        cJSON* ddocJ = cJSON_Parse(source.c_str());
        if(!ddocJ) {
            return;
        }
        cJSON* views = cJSON_GetObjectItem(ddocJ, "views");
        if(!views)
        {
            cJSON_Delete(ddocJ);
            return;
        }
        cJSON *view = views->child;
        while(view != NULL) {
            cJSON* mapfunJ = cJSON_GetObjectItem(view, "map");
            cJSON* redfunJ = cJSON_GetObjectItem(view, "reduce");
            string mapfun("");
            string redfun("");
            if(mapfunJ) {
                mapfun = string(mapfunJ->valuestring);
            }
            if(redfunJ) {
                redfun = string(mapfunJ->valuestring);
            }
            string name = string(view->string);
            pair<string, string> item(mapfun, redfun);
            views_map[name] = item;
            view = view->next;
        }
        cJSON_Delete(ddocJ);
    }

    map<string, pair<string, string> > views_map;
};

typedef map<string, pair<string, string> > ViewMap;

std::list<string> findActiveVBuckets(const string& bucket) {
    std::list<string> activevbs;

    glob_t globres;
    std::stringstream pat;
    pat << bucket << "/" << "*.couch.*";
    glob(pat.str().c_str(), 0, NULL, &globres);
    for(int i = 0; i < globres.gl_pathc; i++) {
        Db* db;
        if(couchstore_open_db(globres.gl_pathv[i], 0, &db) ==
                COUCHSTORE_SUCCESS) {
            LocalDoc* statedoc;
            if(couchstore_open_local_document(db, "_local/vbstate",
                                           sizeof("_local/vbstate") - 1,
                                           &statedoc) == COUCHSTORE_SUCCESS) {
                vbinfo vi;
                if(parseVBState(string(statedoc->json.buf, statedoc->json.size), &vi) == 0) {
                    if(vi.state == ACTIVE) {
                        activevbs.push_back(string(globres.gl_pathv[i]));
                    }
                }
                couchstore_free_local_document(statedoc);
            }
            couchstore_close_db(db);
        }
    }
    globfree(&globres);
    return activevbs;
}

string findDBFile(const string& basepath, const string& bucket) {
    glob_t globres;
    std::stringstream pat;
    pat << bucket << "/" << basepath << ".couch.*";
    glob(pat.str().c_str(), 0, NULL, &globres);
    if(globres.gl_pathc != 1) {
        globfree(&globres);
        return "";
    }

    string dbfile(globres.gl_pathv[0]);
    globfree(&globres);
    return dbfile;
}

void printMapResults(const std::list<map_result_t>& results) {
    std::list<map_result_t>::const_iterator rit = results.begin();
    while(rit != results.end()) {
        cerr << "Emitted item: `" << string((*rit).first.data, (*rit).first.length) << "', `"
             << string((*rit).second.data, (*rit).second.length) << "'" << endl;
        ++rit;
    }
}

string sb2hex(sized_buf* sb) {
    std::stringstream hexss;
    const char *dp = sb->buf;
    const char *end = sb->buf + sb->size;
    char buf[3];
    while(dp < end) {
        sprintf(buf, "%02x", (unsigned char) *dp);
        hexss << buf;
        dp++;
    }

    return hexss.str();
}

string meta_json(DocInfo* di) {
    cJSON* obj = cJSON_CreateObject();
    cJSON_AddItemToObject(obj, "_id",
            cJSON_CreateString(string(di->id.buf, di->id.size).c_str()));

    std::stringstream revss;
    revss << di->rev_seq << "-" << sb2hex(&di->rev_meta);
    cJSON_AddItemToObject(obj, "_rev",
            cJSON_CreateString(revss.str().c_str()));

    uint32_t exptime;
    uint32_t flags;
    memcpy(&exptime, di->rev_meta.buf + 8, 4);
    memcpy(&flags, di->rev_meta.buf + 12, 4);
    cJSON_AddItemToObject(obj, "$expiration", cJSON_CreateNumber(ntohl(exptime)));
    cJSON_AddItemToObject(obj, "$flags", cJSON_CreateNumber(ntohl(flags)));

    char* out = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    string final(out);
    free(out);
    return final;
}

int doc_map_cb(Db* db, DocInfo* di, void* ctx) {
    map_reduce_ctx_t *mctx = static_cast<map_reduce_ctx_t *>(ctx);
    couchstore_error_t errcode;
    Doc* doc;
    errcode = couchstore_open_doc_with_docinfo(db, di, &doc, DECOMPRESS_DOC_BODIES);
    if(errcode != COUCHSTORE_SUCCESS) {
        return errcode;
    }

    string meta = meta_json(di);
    //Splice in the meta data
    std::stringstream docbodyss;
    docbodyss << string(doc->data.buf, doc->data.size - 1) << "," << (meta.c_str() + 1);
    string docbody = docbodyss.str();

    std::list<std::list<map_result_t> > results =
        mapDoc(mctx, json_bin_t((char*) docbody.data(), docbody.length()));
    std::list<std::list<map_result_t> >::iterator rit = results.begin();
    while(rit != results.end()) {
        printMapResults(*rit);
        ++rit;
    }
    return 0;
}

int main(int argc, char** argv) {
    if(argc < 3) {
        cerr << argv[0] << " <bucket dir> <design doc> <output dir>" << endl;
        return -1;
    }

    string bucket(argv[1]);
    string ddocid(argv[2]);
    //string outdir(argv[3]);

    string masterfile = findDBFile("master", bucket);
    cerr << "Master DB: " << masterfile << endl;

    Db* masterdb;
    if(couchstore_open_db(masterfile.c_str(), 0, &masterdb) != COUCHSTORE_SUCCESS) {
        cerr << "Could not open master db." << endl;
        return -2;
    }

    Doc* pDoc;
    if(couchstore_open_document(masterdb, (void*) ddocid.data(), ddocid.length(), &pDoc,
                DECOMPRESS_DOC_BODIES)
            != COUCHSTORE_SUCCESS) {
        cerr << "Could not find design doc: `" << ddocid << "' in master db." << endl;
    }

    string ddoc_src(pDoc->data.buf, pDoc->data.size);
    couchstore_free_document(pDoc);
    couchstore_close_db(masterdb);
    cerr << "Design doc: `" << string(pDoc->data.buf, pDoc->data.size) << "'" << endl;

    DesignDoc ddoc(ddoc_src);
    ViewMap::iterator it = ddoc.views_map.begin();
    std::list<string> mapfuns;
    while(it != ddoc.views_map.end()) {
        cerr << "View: `" << (*it).first << "'" << endl;
        if((*it).second.first.length() > 0) {
            cerr << "Map: `" << (*it).second.first << "'" << endl;
            std::stringstream ss;
            ss << "(" << (*it).second.first << ")";
            mapfuns.push_back(ss.str());
        }
        if((*it).second.second.length() > 0) {
            cerr << "Reduce: `" << (*it).second.second << "'" << endl;
        }
        ++it;
    }

    map_reduce_ctx_t mctx;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    try {
        initContext(&mctx, mapfuns);
        std::list<string> vbs = findActiveVBuckets(bucket);
        std::list<string>::iterator vbit = vbs.begin();
        while(vbit != vbs.end()) {
            Db* vbdb;
            errcode = couchstore_open_db((*vbit).c_str(), 0, &vbdb);
            cerr << "Processing file: " << *vbit << endl;
            if(errcode == COUCHSTORE_SUCCESS) {
                errcode = couchstore_changes_since(vbdb, 0, COUCHSTORE_NO_DELETES,
                                                   doc_map_cb, &mctx);
                couchstore_close_db(vbdb);
            } else {
                cerr << "Couchstore error: " << couchstore_strerror(errcode) << endl;
            }

            if(errcode != COUCHSTORE_SUCCESS) {
                break;
            }
            ++vbit;
        }
    } catch (MapReduceError e) {
        cerr << "mapreduce error: " << e.getMsg() << endl;
    }

    if(errcode != COUCHSTORE_SUCCESS) {
        return -4;
    }

    return 0;
}
