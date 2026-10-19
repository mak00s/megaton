# Credential Store（認証ストア）

Available from Megaton 2.6.0.

1台のMacに1つの置き場（`~/.config/megaton`、または `$MEGATON_HOME`）を作り、Googleの資格情報をすべてそこに集める。コードやAIエージェントはファイルパスではなく**識別子**（`account="sim"`、`sa="corp-ga"`）で資格情報を引くため、repoが変わっても、エージェントが変わっても、同じ手順で認証できる。

## 2系統に分ける理由

| 用途 | 資格情報 | 再認証の頻度 | 識別子 |
|---|---|---|---|
| Workspaceの中身（Docs・Drive・Sheets・Gmail・GA4・GSC・GTM） | ユーザーOAuthトークン（`users/<email>.json`） | Google Cloudのセッション制御の対象外（Cloudスコープを含めない場合）。ただし下記の条件で失効しうる | `account` |
| Google Cloud（BigQuery・Cloud Run など） | サービスアカウント鍵（`sa/<name>.json`） | 期限なし（鍵の削除・無効化、組織ポリシーの鍵有効期限を除く） | `sa` |

gcloudのユーザー認証（`gcloud auth login`）はWorkspace管理者のセッション制御で毎日失効する。APIを叩く用途にはこのストアを使い、gcloudはGCPの操作だけに使う。

**ユーザートークンの失効**: セッション制御の対象外であることは、失効しないことを意味しない。リフレッシュトークンは次の場合に無効になる（[Google公式の失効条件](https://developers.google.com/identity/protocols/oauth2#expiration)）。

- 本人がアカウント設定でアプリのアクセスを取り消した
- 6か月間使われなかった
- パスワードを変更し、トークンにGmailスコープが含まれていた
- 同じOAuthクライアント・同じアカウントで発行数の上限を超え、古いものから無効になった
- Workspace管理者がアプリやスコープを制限した、またはCloudスコープを含むトークンがGoogle Cloudのセッション制御を受けた
- OAuthクライアントのプロジェクトが外部向けかつ公開ステータス「テスト」で、発行から7日が経過した

公開ステータスはGoogle CloudコンソールのOAuth同意画面で確認する。`megaton auth check` の `ok` は確認時点で更新できたことだけを示し、公開ステータスや今後の有効性は保証しない。失効した場合は `check` が `NG` と再認可の `megaton auth login ...` を表示する。

**保存される内容**: トークンの `scopes` は Google が実際に許可した範囲（同意画面で外した分は含まない）。`login` は要求した範囲が許可されなかったとき警告して終了コード1を返す。`import --user-token` は所有者をオンラインで必ず確認し、確認できないトークンは取り込まない。書き込みは一時ファイル＋置換（原子的）。

refresh後も要求scopeを検証する。`granted_scopes=[]` は明示的な空の権限として保存し、
必要なscopeがあればエラーにする。`granted_scopes=None`（応答にscope情報がない場合）
に限り、従来のscope情報を使用する。

## 配置

```
~/.config/megaton/
  credentials.toml        # 別名の台帳（手で編集）
  oauth-client.json       # `megaton auth login` が使うOAuthクライアント（デスクトップ型）
  users/sim@example.com.json
  sa/sa_corp-1872_py-ga-gsc-api.json
```

`credentials.toml`:

```toml
[oauth]
client = "oauth-client.json"

[users]
sim = "sim@example.com"

[sa]
corp-ga = "sa_corp-1872_py-ga-gsc-api.json"
```

鍵ファイルは各Macにコピーして置く（Dropbox等で同期しない）。ディレクトリとファイルは `0700` / `0600`。

## CLI

```bash
megaton auth init                                   # 置き場と台帳の雛形を作る
megaton auth import --oauth-client client.json      # OAuthクライアントを登録
megaton auth import --sa key.json --name corp-ga    # SA鍵を取り込む
megaton auth import --user-token old_token.json     # 既存トークンを取り込む（メールを検証）
megaton auth login --account sim@example.com --scopes docs,drive,sheets
megaton auth list                                   # 何があるか
megaton auth check                                  # 更新できるか（失敗時は直すコマンドを表示）
megaton auth check --account sim                    # 指定アカウントが未登録・失効なら終了コード1
megaton auth token --account sim --scopes docs      # curl用のアクセストークン
megaton auth path --sa corp-ga                      # gspread等にパスを渡したいとき
megaton auth scopes                                 # スコープ別名の一覧
```

`login` は既存トークンのスコープに要求スコープを足して再認可する（1ユーザー1トークン、段階的に拡張）。ブラウザでサインインしたアカウントが `--account` と違えば保存しない。

## Python

```python
from megaton import Megaton
from megaton.auth import CredentialStore

mg = Megaton(sa="corp-ga", headless=True)                       # GA4/GSC/Sheets
mg = Megaton(account="sim", scopes=["sheets"], headless=True)   # ユーザーのトークン

store = CredentialStore()
creds = store.user_credentials("sim", scopes="docs,drive")      # google.oauth2.credentials.Credentials
token = store.user_access_token("sim", "docs")                  # str
path = store.service_account_path("corp-ga")                    # Path（gspread.service_account(filename=path)）
sa_creds = store.service_account_credentials("corp-ga", "sheets,drive")
```

`user_credentials` は要求スコープがトークンに無ければ `CredentialStoreError` を投げる。`hint` に実行すべき `megaton auth login ...` が入るので、エージェントはそれをそのまま案内する。スコープは要求で広がらない（再認可は必ず人がブラウザで行う）。

## curl から使う

```bash
TOKEN=$(megaton auth token --account sim --scopes docs)
curl -s -H "Authorization: Bearer $TOKEN" "https://docs.googleapis.com/v1/documents/<id>?includeTabsContent=true"
```

## 既存のパス指定からの移行ヒント

`Megaton("/path/key.json")` や `megaton query --credential /path/key.json` のようにファイルパスで認証すると、成功後に標準エラーへ1行だけヒントを出す。同じサービスアカウントがストアにあれば `sa="<名前>"`（CLIは `--sa <名前>`）を、なければ `megaton auth import ...` を案内する。OAuthクライアントのファイルなら `megaton auth login` を案内する。

- 同じファイルについては1プロセスで1回だけ。標準出力には出さない（JSON出力を壊さない）
- CI（`CI` / `GITHUB_ACTIONS`）では出さない。ストアはMacごとの置き場でCIには無いため
- `MEGATON_HINTS=0` で止められる
- ヒントの処理が失敗しても認証には影響しない

## スコープ別名

`docs`, `drive`, `drive.file`, `sheets`, `gmail.readonly`, `gmail.modify`, `gmail.compose`, `calendar`, `analytics`, `analytics.edit`, `webmasters`, `tagmanager`, `tagmanager.edit`, `bigquery`, `cloud-platform` など。`megaton auth scopes` で全一覧。上位スコープ（`drive` は `sheets`/`docs` を含む、`gmail.modify` は `gmail.readonly` を含む）は自動で充足と判定する。

## 別のMacへの複製

1. 新しいMacで `pip install megaton` と `megaton auth init`
2. `credentials.toml`・`oauth-client.json`・`users/`・`sa/` を安全な経路（AirDrop等）でコピー
3. `megaton auth check` で全ユーザーが `ok` になることを確認
